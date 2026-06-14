/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::ffi::OsStr;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use backon::{BlockingRetryable, ExponentialBuilder};
use chrono::Utc;
use tokio::process::Command as TokioCommand;
use tokio::time::timeout;
use wait_timeout::ChildExt;

#[derive(thiserror::Error, Debug)]
pub enum CmdError {
    #[error("Invalid retry value {0} for {1}")]
    InvalidRetry(u32, String),
    #[error("Subprocess {0} with arguments {1:?} failed with output: {2}")]
    Subprocess(String, Vec<String>, String),
    #[error(
        "Command execution failed: {command}\nExit code: {exit_code:?}\nStdout: {stdout}\nStderr: {stderr}"
    )]
    CommandExecution {
        command: String,
        exit_code: Option<i32>,
        stdout: String,
        stderr: String,
    },
    #[error("Command timed out after {duration:?}: {command}")]
    Timeout { command: String, duration: Duration },
    #[error("I/O error running {command}: {source}")]
    CommandIo {
        command: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Command {0} with args {1:?} produced output that is not valid UTF8")]
    OutputParse(String, Vec<String>),
    #[error("Error running '{0}': {1:#}")]
    RunError(String, String),
    #[error("Error async running '{0}': {1:#}")]
    TokioRunError(String, String),
}

impl CmdError {
    pub fn subprocess_error(
        command: &std::process::Command,
        output: &std::process::Output,
    ) -> Self {
        let error_details = if output.stderr.is_empty() {
            String::from_utf8_lossy(&output.stdout).to_string()
        } else {
            String::from_utf8_lossy(&output.stderr).to_string()
        };

        Self::Subprocess(
            command.get_program().to_string_lossy().to_string(),
            command
                .get_args()
                .map(|arg| arg.to_string_lossy().to_string())
                .collect::<Vec<String>>(),
            error_details,
        )
    }
    pub fn output_parse_error(command: &Command) -> Self {
        Self::OutputParse(
            command.get_program().to_string_lossy().to_string(),
            command
                .get_args()
                .map(|arg| arg.to_string_lossy().to_string())
                .collect::<Vec<String>>(),
        )
    }

    pub fn command_execution(command: String, output: Output) -> Self {
        Self::CommandExecution {
            command,
            exit_code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        }
    }
}

pub type CmdResult<T> = std::result::Result<T, CmdError>;

/// A cloneable command description that can be rebuilt for retries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
}

impl CommandSpec {
    pub fn new<P: Into<String>>(program: P) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
        }
    }

    pub fn arg<A: Into<String>>(mut self, arg: A) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn args<I, A>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: Into<String>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    pub fn to_command(&self) -> Command {
        let mut command = Command::new(&self.program);
        command.args(&self.args);
        command
    }
}

impl std::fmt::Display for CommandSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.args.is_empty() {
            write!(f, "{}", self.program)
        } else {
            write!(f, "{} {}", self.program, self.args.join(" "))
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommandExecutionOptions {
    pub timeout: Option<Duration>,
    pub retries: u32,
    pub retry_delay: Duration,
    pub max_retry_delay: Duration,
    pub retry_multiplier: f32,
    pub verbose: bool,
}

impl Default for CommandExecutionOptions {
    fn default() -> Self {
        Self {
            timeout: Some(Duration::from_secs(30)),
            retries: 3,
            retry_delay: Duration::from_millis(500),
            max_retry_delay: Duration::from_secs(60),
            retry_multiplier: 2.0,
            verbose: false,
        }
    }
}

impl CommandExecutionOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    pub fn with_max_retry_delay(mut self, max_delay: Duration) -> Self {
        self.max_retry_delay = max_delay;
        self
    }

    pub fn with_retry_multiplier(mut self, multiplier: f32) -> Self {
        self.retry_multiplier = multiplier;
        self
    }

    pub fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }
}

pub struct CommandExecutor<'a> {
    pub options: &'a CommandExecutionOptions,
}

impl<'a> CommandExecutor<'a> {
    pub fn execute_with_retry(&self, command_spec: &CommandSpec) -> CmdResult<Output> {
        if self.options.verbose {
            println!("[EXEC] Executing command with timeout and retry: {command_spec}");
            println!(
                "[EXEC] Retry config: {} attempts, {:.1}x multiplier, {} initial delay, {} max delay",
                self.options.retries + 1,
                self.options.retry_multiplier,
                format_duration(self.options.retry_delay),
                format_duration(self.options.max_retry_delay)
            );
        }

        let backoff = ExponentialBuilder::default()
            .with_min_delay(self.options.retry_delay)
            .with_max_delay(self.options.max_retry_delay)
            .with_max_times(self.options.retries as usize)
            .with_factor(self.options.retry_multiplier);

        let execute_fn = || self.execute_single_attempt(command_spec);

        let result = execute_fn
            .retry(&backoff)
            .when(|err| self.should_retry_error(err))
            .notify(|err, dur| {
                if self.options.verbose {
                    println!(
                        "[RETRY] Retrying after {} due to error: {err}",
                        format_duration(dur)
                    );
                }
            })
            .call();

        match result {
            Ok(output) => {
                if self.options.verbose {
                    println!("[EXEC] Command completed successfully");
                }
                Ok(output)
            }
            Err(error) => {
                if self.options.verbose {
                    println!("[EXEC] Command failed after all retries: {error}");
                }
                Err(error)
            }
        }
    }

    fn execute_single_attempt(&self, command_spec: &CommandSpec) -> CmdResult<Output> {
        let start_time = Instant::now();
        let mut command = command_spec.to_command();
        let command_string = command_spec.to_string();

        let child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|source| CmdError::CommandIo {
                command: command_string.clone(),
                source,
            })?;

        let output = if let Some(timeout) = self.options.timeout {
            self.execute_with_timeout(child, timeout, start_time, command_spec)?
        } else {
            child
                .wait_with_output()
                .map_err(|source| CmdError::CommandIo {
                    command: command_string.clone(),
                    source,
                })?
        };

        if output.status.success() {
            Ok(output)
        } else {
            Err(CmdError::command_execution(command_string, output))
        }
    }

    fn execute_with_timeout(
        &self,
        mut child: Child,
        timeout: Duration,
        start_time: Instant,
        command_spec: &CommandSpec,
    ) -> CmdResult<Output> {
        if self.options.verbose {
            println!(
                "[TIMEOUT] Waiting for command with timeout: {}",
                format_duration(timeout)
            );
        }

        let command_string = command_spec.to_string();
        match child
            .wait_timeout(timeout)
            .map_err(|source| CmdError::CommandIo {
                command: command_string.clone(),
                source,
            })? {
            Some(status) => {
                let execution_time = start_time.elapsed();
                if self.options.verbose {
                    println!(
                        "[TIMEOUT] Command completed in {}",
                        format_duration(execution_time)
                    );
                }

                let stdout = if let Some(mut stdout) = child.stdout.take() {
                    let mut buf = Vec::new();
                    std::io::Read::read_to_end(&mut stdout, &mut buf).map_err(|source| {
                        CmdError::CommandIo {
                            command: command_string.clone(),
                            source,
                        }
                    })?;
                    buf
                } else {
                    Vec::new()
                };

                let stderr = if let Some(mut stderr) = child.stderr.take() {
                    let mut buf = Vec::new();
                    std::io::Read::read_to_end(&mut stderr, &mut buf).map_err(|source| {
                        CmdError::CommandIo {
                            command: command_string.clone(),
                            source,
                        }
                    })?;
                    buf
                } else {
                    Vec::new()
                };

                Ok(Output {
                    status,
                    stdout,
                    stderr,
                })
            }
            None => {
                let execution_time = start_time.elapsed();
                if self.options.verbose {
                    println!(
                        "[TIMEOUT] Command timed out after {}, killing process",
                        format_duration(execution_time)
                    );
                }

                let _ = child.kill();
                let _ = child.wait();

                Err(CmdError::Timeout {
                    command: command_string,
                    duration: timeout,
                })
            }
        }
    }

    pub fn should_retry_error(&self, error: &CmdError) -> bool {
        matches!(
            error,
            CmdError::CommandExecution { .. }
                | CmdError::Timeout { .. }
                | CmdError::CommandIo { .. }
                | CmdError::RunError(_, _)
                | CmdError::TokioRunError(_, _)
        )
    }
}

fn format_duration(duration: Duration) -> String {
    if duration.as_secs() > 0 {
        format!("{}s", duration.as_secs())
    } else {
        format!("{}ms", duration.as_millis())
    }
}

#[derive(Debug)]
pub struct Cmd {
    command: Command,
    attempts: u32,
    ignore_return: bool,
}

#[derive(Debug)]
pub struct CmdOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub start_time: chrono::DateTime<Utc>,
    pub end_time: chrono::DateTime<Utc>,
}
impl Cmd {
    pub fn new<S: AsRef<OsStr>>(program: S) -> Self {
        Self {
            command: Command::new(program),
            attempts: 1,
            ignore_return: false,
        }
    }

    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.command.args(args);
        self
    }

    pub fn env<S>(mut self, key: S, value: S) -> Self
    where
        S: AsRef<OsStr>,
    {
        self.command.env(key, value);
        self
    }

    pub fn attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }

    pub fn ignore_return(mut self, ignore: bool) -> Self {
        self.ignore_return = ignore;
        self
    }

    pub fn output(mut self) -> CmdResult<String> {
        if cfg!(test) {
            return Ok("test string".to_string());
        }

        let mut last_output = None;
        for _attempt in 0..self.attempts {
            let output = self
                .command
                .output()
                .map_err(|x| CmdError::RunError(self.pretty_cmd(), x.to_string()))?;

            last_output = Some(output.clone());

            if output.status.success() || self.ignore_return {
                return String::from_utf8(output.stdout)
                    .map_err(|_| CmdError::output_parse_error(&self.command));
            }

            // Give some breathing time.
            std::thread::sleep(Duration::from_millis(100));
        }
        if let Some(output) = last_output {
            Err(CmdError::subprocess_error(&self.command, &output))
        } else {
            Err(CmdError::InvalidRetry(self.attempts, self.pretty_cmd()))
        }
    }

    fn pretty_cmd(&self) -> String {
        format!(
            "{} {}",
            self.command.get_program().to_string_lossy(),
            self.command
                .get_args()
                .map(|x| x.to_string_lossy())
                .collect::<Vec<std::borrow::Cow<'_, str>>>()
                .join(" ")
        )
    }
}

/// Async implementation of Cmd.
#[derive(Debug)]
pub struct TokioCmd {
    command: TokioCommand,
    attempts: u32,
    timeout: u64,
}

impl TokioCmd {
    pub fn new<S: AsRef<OsStr>>(program: S) -> Self {
        Self {
            command: TokioCommand::new(program),
            attempts: 1,
            timeout: 3600,
        }
    }

    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.command.args(args);
        self
    }

    pub fn env<S>(mut self, key: S, value: S) -> Self
    where
        S: AsRef<OsStr>,
    {
        self.command.env(key, value);
        self
    }

    pub fn attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }

    pub fn timeout(mut self, timeout: u64) -> Self {
        self.timeout = timeout;
        self
    }

    pub async fn output_with_timeout(mut self) -> CmdResult<CmdOutput> {
        if cfg!(test) {
            return Ok(CmdOutput {
                stdout: "test string".to_string(),
                stderr: "test string".to_string(),
                exit_code: 0,
                start_time: Utc::now(),
                end_time: Utc::now(),
            });
        }
        let mut last_output = None;
        let start_time = Utc::now();

        for _attempt in 0..self.attempts {
            let child = self
                .command
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| CmdError::RunError(self.pretty_cmd(), e.to_string()))?;

            // Apply timeout and run command
            let output = timeout(Duration::from_secs(self.timeout), child.wait_with_output())
                .await
                .map_err(|x| CmdError::TokioRunError(self.pretty_cmd(), x.to_string()))?
                .map_err(|y| CmdError::TokioRunError(self.pretty_cmd(), y.to_string()))?;
            last_output = Some(output.clone());

            if output.status.success() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let end_time = Utc::now();
        // Here idea is to capture both std out and std err along with exit code
        if let Some(output) = last_output {
            Ok(CmdOutput {
                stdout: String::from_utf8(output.stdout)
                    .map_err(|_| CmdError::output_parse_error(self.command.as_std()))?,
                stderr: String::from_utf8(output.stderr)
                    .map_err(|_| CmdError::output_parse_error(self.command.as_std()))?,
                exit_code: output.status.code().unwrap_or_default(),
                start_time,
                end_time,
            })
        } else {
            Err(CmdError::InvalidRetry(self.attempts, self.pretty_cmd()))
        }
    }

    pub async fn output(mut self) -> CmdResult<String> {
        if cfg!(test) {
            return Ok("test string".to_string());
        }

        let mut last_output = None;
        for _attempt in 0..self.attempts {
            let output = self
                .command
                .output()
                .await
                .map_err(|x| CmdError::TokioRunError(self.pretty_cmd(), x.to_string()))?;

            last_output = Some(output.clone());

            if output.status.success() {
                return String::from_utf8(output.stdout)
                    .map_err(|_| CmdError::output_parse_error(self.command.as_std()));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if let Some(output) = last_output {
            Err(CmdError::subprocess_error(self.command.as_std(), &output))
        } else {
            Err(CmdError::InvalidRetry(self.attempts, self.pretty_cmd()))
        }
    }

    fn pretty_cmd(&self) -> String {
        let c = self.command.as_std();
        format!(
            "{} {}",
            c.get_program().to_string_lossy(),
            c.get_args()
                .map(|x| x.to_string_lossy())
                .collect::<Vec<std::borrow::Cow<'_, str>>>()
                .join(" ")
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use carbide_test_support::value_scenarios;

    use super::*;

    #[test]
    fn command_spec_displays_program_and_args() {
        value_scenarios!(
            run = |spec| format!("{spec}");
            "program only" {
                CommandSpec::new("echo") => "echo".to_string(),
            }

            "program with args" {
                CommandSpec::new("echo").args(["hello", "world"]) => "echo hello world".to_string(),
            }
        );
    }

    #[test]
    fn command_executor_runs_successful_spec() {
        let options = CommandExecutionOptions::new()
            .with_timeout(Some(Duration::from_secs(5)))
            .with_retries(0);
        let executor = CommandExecutor { options: &options };
        let spec = CommandSpec::new("echo").arg("hello");

        let output = executor.execute_with_retry(&spec).unwrap();

        assert!(output.status.success());
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "hello");
    }

    #[test]
    fn command_executor_reports_failure_details() {
        let options = CommandExecutionOptions::new()
            .with_timeout(Some(Duration::from_secs(5)))
            .with_retries(0);
        let executor = CommandExecutor { options: &options };
        let spec = CommandSpec::new("sh").args(["-c", "printf stdout; printf stderr >&2; exit 7"]);

        let error = executor.execute_with_retry(&spec).unwrap_err();

        match error {
            CmdError::CommandExecution {
                command,
                exit_code,
                stdout,
                stderr,
            } => {
                assert_eq!(command, "sh -c printf stdout; printf stderr >&2; exit 7");
                assert_eq!(exit_code, Some(7));
                assert_eq!(stdout, "stdout");
                assert_eq!(stderr, "stderr");
            }
            other => panic!("expected command execution error, got {other:?}"),
        }
    }

    #[test]
    fn command_executor_reports_timeout() {
        let options = CommandExecutionOptions::new()
            .with_timeout(Some(Duration::from_millis(50)))
            .with_retries(0);
        let executor = CommandExecutor { options: &options };
        let spec = CommandSpec::new("sleep").arg("1");

        let error = executor.execute_with_retry(&spec).unwrap_err();

        match error {
            CmdError::Timeout { command, duration } => {
                assert_eq!(command, "sleep 1");
                assert_eq!(duration, Duration::from_millis(50));
            }
            other => panic!("expected timeout error, got {other:?}"),
        }
    }
}
