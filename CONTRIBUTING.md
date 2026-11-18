# How to contribute to this repo

We love your input! We want to make contributing to this project as easy and transparent as possible, whether it's:

- Reporting a bug
- Proposing new features
- Discussing the current state of the code
- Submitting a fix

This is how you should do it:

Use our [issue list](../../issues) to report a **bug** or **propose a new feature**, including

### Reporting a **bug report**  
- A quick summary and/or background
- Steps to reproduce
- What actually happens

### Adding a **feature request**  
- Brief description of the feature
- What problem/issue will this solve
- A sort of Definition of Done - "How should it look when finished"

### When submitting a fix using a Pull Request (PR)  
- Fork this repository from GitHub  
- After you have made the changes, create a pull request (PR)  

We will review the pull request and if it is appropriate and there are no clashes or vulnerabilities, it will be merged to the main code.

## Packaging

Release Please updates `compat/pyproject.toml` in each release PR. Keep the
`x-release-please-version` markers on the alias version and dependency pins.
On release builds, `setuptools-scm` gets Explorer's version from the Git tag.
On ordinary PRs, the build-and-test workflow builds both packages without
requiring their versions to match. The publishing workflow runs only on
published GitHub releases. Before uploading to PyPI, it checks that both wheels
have the same version and that the alias pins that exact Explorer version.

To build the compatibility package locally with `build` installed:

```bash
cp LICENSE compat/LICENSE
python -m build --outdir dist/compat compat
```