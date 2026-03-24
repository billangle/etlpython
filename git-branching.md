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
git commit -m "CPS-POPSUP-7557: follow-up update"
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
