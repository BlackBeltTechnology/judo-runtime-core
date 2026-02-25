# Development Version and Branch Handling

This document describes the branching strategy, versioning policy, and CI/CD workflows for JUDO Runtime Core.

## Branches

The versioning policy follows [GitFlow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow):

| Branch Pattern | Purpose |
|----------------|---------|
| `develop` | Latest development sources of the current active version |
| `feature/JNG-NUMBER_short_summary` | Feature branches based on `develop` for new features |
| `release/X.Y.Z` or `X_Y_betaN` | Release branches (the `release/` prefix is reserved for CI) |
| `bugfix/JNG-NUMBER_short_summary` | Bug fixes based on release branches; must be applied to release and development branches of newer versions |
| `support/JNG-NUMBER_short_summary` | Support branches based on release branches; same merge-forward rules as bugfix |
| `master` | Latest released sources of the current active version |

```mermaid
gitGraph
    commit id: "initial"
    branch develop
    commit id: "dev-1"
    branch feature/JNG-1
    commit id: "feat-1"
    commit id: "feat-2"
    checkout develop
    merge feature/JNG-1 id: "merge-feat"
    branch release/1.0-beta1
    commit id: "rc-1"
    branch bugfix/JNG-4
    commit id: "fix-1"
    checkout release/1.0-beta1
    merge bugfix/JNG-4 id: "merge-fix"
    checkout main
    merge release/1.0-beta1 id: "release-1.0"
    checkout develop
    merge release/1.0-beta1 id: "merge-back"
    commit id: "dev-2"
```

## Version Numbers

Versions follow semantic versioning with these rules:

- **Feature branches:** Do not change version numbers
- **`develop` branch:** 2nd number incremented when a release branch starts
- **Bugfix branches:** No version change — applied on release branches during pre-release testing
- **Support branches:** 3rd number incremented when started; merged back to release branch on update release (without merging to master)
- **Hotfix branches:** 4th number incremented when started; applied to both release and master branches

## GitHub Actions Workflows

### build.yml — Main Build Pipeline

Triggered on pushes to `develop` and pull requests to `develop`, `master`, `increment/*`, and `release/*`.

```mermaid
flowchart TD
    START[Push or PR event] --> BRANCH_CHECK{Branch type?}

    BRANCH_CHECK -->|master, release/*| RELEASE_VER[Version from pom.xml<br/>without -SNAPSHOT]
    BRANCH_CHECK -->|develop, increment/*| DEV_VER[Version:<br/>major.minor.qualifier.date_commitId_branch]

    RELEASE_VER --> BUILD[Build & Deploy to Nexus]
    DEV_VER --> BUILD

    BUILD --> TAG[Create git tag<br/>v&lt;version&gt;]

    TAG --> BRANCH_CHECK2{Branch type?}

    BRANCH_CHECK2 -->|increment/*, release/*| MERGE_TAG[Create tag<br/>merge-pr/&lt;version&gt;]
    MERGE_TAG --> TRIGGER_MERGE[Trigger merge-pr-tagged.yml]

    BRANCH_CHECK2 -->|develop| CHANGELOG[Build changelog]
    CHANGELOG --> GH_RELEASE[Create GitHub pre-release]
```

### merge-pr-tagged.yml — PR Merge Automation

Triggered when a `merge-pr/*` tag is pushed. Handles automatic merging of PRs to the correct target branch.

```mermaid
flowchart TD
    START[merge-pr/* tag pushed] --> GET_VER[Extract version from tag]
    GET_VER --> FORMAT_CHECK{Version format?}

    FORMAT_CHECK -->|major.minor.qualifier| MERGE_MASTER[Merge PR to master]
    MERGE_MASTER --> TRIGGER_RELEASE[Trigger create-release-on-master.yml]

    FORMAT_CHECK -->|other| SQUASH_DEV[Squash PR to develop]
    SQUASH_DEV --> TRIGGER_BUILD[Trigger build.yml]

    TRIGGER_RELEASE --> CLEANUP[Delete merge-pr/&lt;version&gt; tag]
    TRIGGER_BUILD --> CLEANUP
```

### create-release-on-master.yml — Release Creation

Triggered on pushes to `master`. Creates a GitHub release with a generated changelog.

```mermaid
flowchart LR
    PUSH[Push to master] --> VERSION[Get version from tag]
    VERSION --> CHANGELOG[Build changelog]
    CHANGELOG --> RELEASE[Create GitHub release<br/>marked as latest]
```

### release.yml — Manual Release Trigger

Manually triggered with a version parameter (`auto` or a specific `major.minor.qualifier` version).

```mermaid
flowchart TD
    TRIGGER[Manual trigger<br/>with version param] --> AUTO_CHECK{Version = 'auto'?}

    AUTO_CHECK -->|yes| POM_VER[Release version from pom.xml<br/>without -SNAPSHOT]
    AUTO_CHECK -->|no| GIVEN_VER[Release version = given version]

    POM_VER --> NEXT[Next version = qualifier + 1]
    GIVEN_VER --> NEXT

    NEXT --> PR_MASTER[Create PR to master<br/>with release version]
    NEXT --> PR_DEVELOP[Create PR to develop<br/>with next version]

    PR_MASTER --> BUILD1[Trigger build.yml]
    PR_DEVELOP --> BUILD2[Trigger build.yml]
```

## Development Rules

> **Important:** There is no commit without a ticket number. Every pull request and commit must include a JIRA reference in the format `JNG-xxx`.

Issue tracking is managed in [JIRA](https://blackbelt.atlassian.net/jira/dashboards).
