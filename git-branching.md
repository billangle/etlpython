# Git Branching and Automated DEV Builds

This document defines the standard Git and branch workflow for all ETL projects in this repository.

## Branch Naming Standard

Create feature branches using:

`<PROJECT>-<JIRA-TICKET>`

Example for CPS:

`CPS-POPSUP-7557`

Use the project code for the pipeline you are changing (for example `CPS`, `ECP`, `DMC`, `CNSV`, and so on).
`CPS` is used in commands as an example only; replace it with your assigned project code.

## DEV Automation Flow (FPACDEV)

1. Create and push a correctly named feature branch.
2. BitBucket webhook is triggered by that branch activity.
3. CI builds and deploys that project's changes to the DEV environment using `FPACDEV` naming.

## Promotion to PROD

1. Open a pull request from the feature branch into `main`.
2. Complete review and approvals.
3. Merge into `main`.
4. `main` is the promotion path for production deployment.

## Git Commands: End-to-End Feature Workflow

> **Important:** Replace `CPS` in all branch and commit examples with your assigned project code. Examples: `ECP-POPSUP-7557`, `DMC-POPSUP-8123`, `CNSV-POPSUP-9001`.

```bash
# 1) Start from latest main
git checkout main
git pull origin main

# 2) Create feature branch (example)
# Replace CPS with your assigned project code
git checkout -b CPS-POPSUP-7557

# 3) Work and commit
git add .
git commit -m "CPS-POPSUP-7557: describe change"

# 4) Push branch (triggers BitBucket webhook/DEV pipeline)
git push -u origin CPS-POPSUP-7557

# 5) Continue updates on same branch
git add .
git commit -m "POPSUP-7557: follow-up update"
git push
```

## Daily Main-Into-Feature Sync (Avoid Merge Conflicts)

Run this daily while the feature branch is open:

```bash
# Fetch latest remote refs
git fetch origin

# Update local main
git checkout main
git pull origin main

# Merge main into your feature branch
# Replace CPS with your assigned project code
git checkout CPS-POPSUP-7557
git merge main

# If conflicts occur:
#   1) resolve files
#   2) git add <resolved files>
#   3) git commit

# Push updated feature branch
git push
```

If you prefer not to switch branches repeatedly, this is equivalent:

```bash
git checkout CPS-POPSUP-7557
git fetch origin
git merge origin/main
git push
```

## Create Pull Request

```bash
# Switch to your feature branch
# Replace CPS with your assigned project code and POPSUP-7557 with your Jira ticket
git checkout CPS-POPSUP-7557

# Ensure your branch is current with main and pushed
git fetch origin
git merge origin/main
git push

# Optional review check before opening PR
git log --oneline origin/main..HEAD
```

Create the pull request in BitBucket:
1. Source branch: `CPS-POPSUP-7557` (replace with your project/Jira branch)
2. Target branch: `main`
3. Add reviewers and complete PR details, then submit

## Delete Feature Branch

After the pull request is merged, delete the feature branch both locally and remotely.

```bash
# Switch to main and sync
git checkout main
git pull origin main

# Delete local feature branch
# Replace CPS with your assigned project code and POPSUP-7557 with your Jira ticket
git branch -d CPS-POPSUP-7557

# Delete remote feature branch
git push origin --delete CPS-POPSUP-7557
```

## Helpful Git Commands

Use these commands during day-to-day feature development, syncing, and pull-request preparation.

### `git status`

```bash
git status
```

- Output: current branch name, staged files, unstaged files, and untracked files.
- Value: quickest way to confirm what will be committed and whether your working tree is clean before a commit, merge, or PR.

### `git fetch origin`

```bash
git fetch origin
```

- Output: updates remote-tracking refs such as `origin/main` and `origin/<feature-branch>`.
- Value: safely refreshes what exists on remote without changing local files; ideal before merge/rebase decisions.

### `git branch --show-current`

```bash
git branch --show-current
```

- Output: a single line with your current branch name.
- Value: avoids accidental commits/pushes to the wrong branch.

### `git log --oneline --decorate --graph -20`

```bash
git log --oneline --decorate --graph -20
```

- Output: compact commit history with branch/tag labels and merge graph for the last 20 commits.
- Value: quickly visualizes branch divergence and confirms whether main has already been merged into your feature branch.

### `git diff`

```bash
git diff
```

- Output: line-by-line unstaged changes compared to the last commit.
- Value: review exact edits before staging.

### `git diff --staged`

```bash
git diff --staged
```

- Output: line-by-line staged changes that will go into the next commit.
- Value: final safety check before `git commit`.

### `git add -p`

```bash
git add -p
```

- Output: interactive hunk-by-hunk prompt for staging selected changes.
- Value: create clean, focused commits instead of bundling unrelated edits.

### `git log --oneline origin/main..HEAD`

```bash
git log --oneline origin/main..HEAD
```

- Output: commits that are in your current branch but not in `origin/main`.
- Value: verifies exactly what your pull request will include.

### `git restore --staged <file>`

```bash
git restore --staged <file>
```

- Output: removes `<file>` from staging while keeping local edits in your working directory.
- Value: undo accidental staging without losing work.

### `git stash push -m "wip"` and `git stash pop`

```bash
git stash push -m "wip"
git stash pop
```

- Output: temporarily stores uncommitted changes, then reapplies them later.
- Value: lets you quickly switch branches or pull latest main without committing incomplete work.

### `git remote -v`

```bash
git remote -v
```

- Output: remote names and URLs (fetch/push), usually `origin`.
- Value: confirms you are pushing to the expected BitBucket repository.
