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

// src/executor.rs
// Command executor adapts the shared command runner to mlxconfig-specific
// concerns like temporary JSON files, dry-run logging, and confirmation prompts.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Output;

use carbide_utils::cmd::CommandExecutor as SharedCommandExecutor;
use uuid::Uuid;

use crate::mlxconfig_runner::command_builder::CommandSpec;
use crate::mlxconfig_runner::{ExecOptions, MlxRunnerError};

// CommandExecutor handles the mlxconfig-specific parts of command execution
// while delegating process spawning, retries, and timeouts to carbide-utils.
pub struct CommandExecutor<'a> {
    // options contains the execution options controlling retry,
    // timeout, and interactive confirmation behavior.
    pub options: &'a ExecOptions,
}

impl<'a> CommandExecutor<'a> {
    // Executes a command with shared retry and timeout handling.
    pub fn execute_with_retry(&self, command_spec: &CommandSpec) -> Result<Output, MlxRunnerError> {
        let command_options = self.options.command_execution_options();
        let executor = SharedCommandExecutor {
            options: &command_options,
        };

        executor
            .execute_with_retry(command_spec)
            .map_err(MlxRunnerError::from)
    }

    // Determines whether an error should trigger a retry or is permanent.
    // Currently treats I/O errors and command execution failures as transient,
    // but treats specific errors like VariableNotFound as permanent.
    pub fn should_retry_error(&self, error: &MlxRunnerError) -> bool {
        match error {
            // These errors are likely permanent and shouldn't be retried
            MlxRunnerError::VariableNotFound { .. } => false,
            MlxRunnerError::ArraySizeMismatch { .. } => false,
            MlxRunnerError::ValueConversion { .. } => false,
            MlxRunnerError::InvalidArrayIndex { .. } => false,
            MlxRunnerError::DeviceMismatch { .. } => false,
            MlxRunnerError::NoDeviceFound => false,
            MlxRunnerError::ConfirmationDeclined { .. } => false,
            MlxRunnerError::JsonParsing { .. } => false,

            // These errors might be transient and worth retrying
            MlxRunnerError::CommandExecution { .. } => true,
            MlxRunnerError::TempFileError { .. } => true,
            MlxRunnerError::Timeout { .. } => true,
            MlxRunnerError::Io(_) => true,
            MlxRunnerError::GenericError(_) => true,
        }
    }

    // Prompts the user for confirmation when modifying "destructive" variables.
    // Returns true if the user confirms, false if they decline.
    pub fn prompt_for_confirmation(
        &self,
        destructive_vars: &[String],
    ) -> Result<bool, MlxRunnerError> {
        println!("WARNING: You are about to modify destructive variables:");
        for var in destructive_vars {
            println!(" - {var}");
        }
        println!();
        print!("Continue? (y/N): ");

        std::io::stdout().flush().map_err(MlxRunnerError::Io)?;

        let mut input = String::new();
        std::io::stdin()
            .read_line(&mut input)
            .map_err(MlxRunnerError::Io)?;

        let response = input.trim().to_lowercase();
        let confirmed = response == "y" || response == "yes";

        if self.options.verbose {
            println!(
                "[CONFIRM] User {} destructive operation",
                if confirmed { "confirmed" } else { "declined" }
            );
        }

        Ok(confirmed)
    }

    // Creates a temporary file for mlxconfig JSON output using the specified
    // prefix (which defaults to /tmp) and a UUID for uniqueness.
    pub fn create_temp_file(&self, prefix: &str) -> Result<PathBuf, MlxRunnerError> {
        let filename = format!("mlxconfig-runner-{}.json", Uuid::new_v4());
        let path = Path::new(prefix).join(filename);

        fs::File::create(&path).map_err(|e| MlxRunnerError::temp_file_error(path.clone(), e))?;

        if self.options.verbose {
            println!("[TEMP] Created temporary file: {}", path.display());
        }

        Ok(path)
    }

    // Cleans up the temporary mlxconfig JSON output file if it exists.
    // Safe to call even if the file doesn't exist.
    pub fn cleanup_temp_file(&self, temp_file: &Path) -> Result<(), MlxRunnerError> {
        if temp_file.exists() {
            fs::remove_file(temp_file)
                .map_err(|e| MlxRunnerError::temp_file_error(temp_file.to_path_buf(), e))?;

            if self.options.verbose {
                println!("[TEMP] Cleaned up temporary file: {}", temp_file.display());
            }
        }
        Ok(())
    }

    // Executes a dry-run operation by just logging what would be executed.
    // Used when dry_run is enabled in ExecOptions.
    pub fn execute_dry_run(&self, command_spec: &CommandSpec, operation_type: &str) {
        println!("[DRY RUN] Would execute {operation_type}: {command_spec}");
    }

    // Returns whether the current executor run is configured for dry-run mode.
    pub fn is_dry_run(&self) -> bool {
        self.options.dry_run
    }

    // Returns whether the current executor run is configured with verbose logging.
    pub fn is_verbose(&self) -> bool {
        self.options.verbose
    }
}
