# How to Write Documentation in infra-controller

Conventions for writing and updating documentation in this repository: READMEs,
guides, reference pages, release notes, and design-doc prose. The goal is docs
that read consistently no matter who writes them. For Rust code style, refer to
[`STYLE_GUIDE.md`](STYLE_GUIDE.md).

Contributors and AI agents can skim this before writing or editing docs; the
Quick Checklist at the end is the fast pass. These are guidelines, not gates.
When a rule fights clarity, clarity wins.

## Scope

Applies to Markdown documentation in this repo: READMEs, guides, tutorials,
reference pages, design-doc prose, and release notes. Generated reference output
and inline code comments are out of scope.

## Voice and Tone

Write in a clear, direct voice: active voice, present tense, and plain words.
Address the reader as "you", and stay conversational without being chatty. A
useful test: if you would not say it that way to a colleague, do not write it.
Prefer the simpler word ("use", not "leverage"), use more periods, and if you can
cut a word, cut it.

## Word Choice

Write for a global audience, including non-native English speakers and
translation tools: short sentences, active voice, and straightforward language.
Avoid culture-specific idioms, humor, and puns.

### Plainer Alternatives

Prefer the clearer word:

| Avoid  | Use Instead              |
|--------|--------------------------|
| see    | refer to                 |
| may    | can (for possibility)    |
| once   | after (for sequence)     |
| please | (omit in technical docs) |

### Latin Abbreviations

Replace Latin abbreviations with plain English:

| Avoid | Use Instead          |
|-------|----------------------|
| e.g.  | for example, such as |
| i.e.  | that is              |
| etc.  | and so on            |
| via   | by, through, using   |
| vs.   | compared to          |

### Idioms and Jargon

Prefer the precise technical term over a vivid one. This applies to prose,
headings, and example narration; code, command names, and quoted output are
exempt. Common swaps:

| Avoid            | Use Instead                   |
|------------------|-------------------------------|
| bake, baked into | embed, embedded at build time |
| smoke test       | basic verification test       |
| footgun          | common pitfall                |
| spin up          | start                         |

### Contractions

Avoid contractions in documentation: "it is", not "it's"; "cannot", not "can't".

### Acronyms

- Spell out an acronym on first use unless it is common in this domain (for
  example, CPU, GPU, DPU, BMC, API, DNS, DHCP, PXE): write "full term (ABBR)",
  then use the abbreviation.
- Pluralize acronyms with a bare `s`: GPUs, not GPU's.

## Numbers and Units

- Spell out zero through nine in prose; use numerals for 10 and up. Use numerals
  for exact values regardless of size: "set it to 5", "3 retries", port 8080.
- Use a thousands separator: 1,397.
- Do not start a sentence with a numeral; rewrite or spell it out.
- Spell out ordinals: "tenth", not "10th".
- Put a space between a number and its unit (40 GB, 30 ms) and be consistent
  within a document. Use conventional speed and throughput units: GB/s, 100G,
  100GbE.
- Dates: use an unambiguous format, either ISO 8601 (2025-08-12) or "August 12,
  2025"; do not use ordinals.

## Capitalization

- Use one heading style consistently across a document. This guide uses title
  case; sentence case is equally acceptable if applied consistently.
- Do not put code, italics, quotes, ampersands, or exclamation marks in headings.
- Keep proper nouns and product names in their canonical casing: `PostgreSQL`,
  `Kubernetes`, `NVIDIA` (all caps).

## Punctuation

- **Em dashes**: avoid them (U+2014). Use a spaced hyphen, a colon, a semicolon,
  or parentheses instead.
- **En dashes**: use for numeric ranges, with no spaces (2015–2017).
- **Hyphens**: hyphenate compound modifiers before a noun (built-in tool,
  real-time data); do not hyphenate the same words used as a noun, and do not
  hyphenate `-ly` adverbs.
- **Commas**: use the serial (Oxford) comma, and follow an introductory phrase
  with a comma.
- **Apostrophes**: never use one for the possessive "its" or to pluralize.
- **Quotation marks**: use double quotes, with periods and commas inside them.
- **Semicolons**: prefer splitting into two sentences.
- **Exclamation marks**: avoid.
- **Slashes**: avoid "and/or"; a slash is fine for established pairs (read/write)
  and paths.
- **Ampersands**: write "and", not "&", unless it is part of a name.

## Grammar

- **Active voice**: prefer it. Passive is fine when the actor is unknown or
  unimportant.
- **Present tense**: "the service returns", not "the service will return".
- **Second person**: address the reader as "you", not "the user" or "we".
- **That and which**: "that" introduces an essential clause (no commas); "which"
  introduces a nonessential one (with commas).

## Code and Formatting

Format these elements consistently:

| Element                  | Format          | Example                   |
|--------------------------|-----------------|---------------------------|
| Commands, files, paths   | Monospace       | `apt-get install`         |
| Variables in paths       | Angle brackets  | `/home/<username>/.login` |
| Identifiers, config keys | Monospace       | `max_retries`             |
| UI elements              | Bold            | **Save As** > **Close**   |
| New terms                | Italic          | *idempotent*              |
| Error messages, strings  | Quotation marks | "Invalid input"           |
| Keyboard shortcuts       | No formatting   | Ctrl+Alt+Delete           |

- Introduce a code block, list, table, or image with a complete sentence; do not
  let it finish the sentence.
- Call it a "code example" or "example", not a "snippet".
- Use fenced code blocks with a language tag; keep standalone or multi-line
  output in a code block and inline error text in quotation marks.
- File extensions are lowercase with a dot (`.tgz`); file types are uppercase
  with no dot (TGZ).
- Use one term for one concept throughout a document.

## Lists and Tables

- Introduce a list or table with a lead-in sentence (a colon for a list, a full
  sentence for a table).
- Use parallel construction, capitalize the first word of each item, and add end
  punctuation only when items are full sentences.
- Keep lists to two levels at most.
- Give tables title-case headers and avoid empty cells.

## Procedures

- Numbered steps are optional; use them for sequences that must run in order.
- Keep a numbered procedure to roughly five to seven steps, and break longer ones
  into subtasks under subheadings.
- Use imperative sentences for actions ("Run the migration") and declarative
  sentences for explanation.

## Links

- Use descriptive link text that names the destination; do not use bare URLs or
  "here", "read more", or "click here" in running text.
- Limit inline links per paragraph so the prose stays readable.

## Readability

- Aim for sentences under 30 words and short paragraphs.
- Use simple, direct language and keep content scannable with clear headings and
  lists.
- Write inclusively; use "they" for a generic person and avoid expressions that
  assume a specific background.

## Applying These Rules

These are guidelines for clarity, not a find-and-replace pass. Before
"correcting" something, confirm it actually improves the doc. Leave these alone:

- Code identifiers, config keys, and paths (`snake_case`, `/opt/...`).
- Quoted output, logs, and API fields: literal text stays literal.
- Proper nouns and product names in their canonical casing.

## Quick Checklist

- [ ] Active voice, present tense, second person ("you").
- [ ] No contractions; no Latin abbreviations ("e.g.", "i.e."); no vivid idioms ("spin up").
- [ ] No em dashes; use a hyphen, colon, or parentheses.
- [ ] Acronyms spelled out on first use; numbers and units consistent.
- [ ] Serial commas; periods inside quotation marks.
- [ ] Commands, paths, and identifiers in monospace; fenced code blocks tagged with a language.
- [ ] Descriptive link text, no bare URLs.
- [ ] Short sentences; one consistent heading style.
