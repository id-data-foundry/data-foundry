# Contributing to DataFoundry

Data Foundry is an openly developed Data Collection platform, this means that everyone (Including you!) is able to contribute to the development of the platform. Involving yourself with the platform can be as easy as sending bug reports, or helping us rewrite the documentation or giving us design tips. 

If you want to contribute to the codebase itself we created a guideline on how to submit your code properly, so that it will remain easy for us to maintain the codebase and make it easier for you to contribute! This document provides a set of guidelines for contributing to DataFoundry. These are mostly guidelines, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request.

## How Can I Contribute?
Before you post your own issue, check for similar Bugs, Improvements or Feature Requests. If the desired feature has not been requested yet, feel free to create an issue yourself.

Please use the issue templates we have provided for you, and make sure to clearly describe what you want. Sometimes it helps to provide example use-cases for us to understand your feature. As Data Foundry was build in a design context, simple sketches might help too! Below you can find some more specific examples.

### Reporting Bugs
This section guides you through submitting a bug report for DataFoundry. Following these guidelines helps maintainers and the community understand your report, reproduce the behavior, and find related reports.

1.   **Use the GitHub Issues tracker** to report bugs.
2.  **Search existing issues** to see if the problem has already been reported. If it has and the issue is still open, add a comment to the existing issue instead of opening a new one.
3.   **Provide a clear and descriptive title.**
4.  **Describe the steps to reproduce the bug** in as much detail as possible.
5.  **Explain what you expected to happen** and what happened instead.
6.  **Include details about your environment**, such as your operating system, Docker version, browser, etc.

### Suggesting Enhancements

This section guides you through submitting an enhancement suggestion for DataFoundry, including completely new features and minor improvements to existing functionality.

1.   **Use the GitHub Issues tracker** to suggest enhancements.
2.  **Provide a clear and descriptive title.**
3.  **Provide a step-by-step description of the suggested enhancement** in as much detail as possible.
4.   **Explain why this enhancement would be useful** to most DataFoundry users.

## Code contributions
Before you get started on building your feature, it might be nice to first open a feature request and discuss your feature there beforehand. This way we can guide you better in the development process and make sure that the platform stays cohesive, preventing feature creep. After that you're ready to go.
To get started;

1. Fork the repository
2. Implement your feature, Make sure to follow the code style and test your features.
3. Update the Documentation and maybe build an example
4. Push to a different branch and make a pull request

### Pull Requests

The process described here has several goals:
- Maintain DataFoundry's quality
- Fix problems that are important to users
- Engage the community in working toward the best possible DataFoundry
- A streamlined process for reviewers

Please follow these steps to have your contribution considered by the maintainers:

1.  **Fork the repository** and create your branch from `main`.
2.  If you've added code that should be tested, **add tests**.
3.  Ensure the test suite passes (`sbt test`).
4.  Make sure your code lints.
5.  Issue that pull request!

## Development Workflow
1.  Fork the `data-foundry` repo.
2.  Clone your fork locally: `git clone https://github.com/YOUR_USERNAME/data-foundry.git`
3.  Create a new branch for your changes: `git checkout -b your-feature-branch-name`
4.  Make your changes. Please try and follow the project's coding style. We have included a development container and a nix-shell script to make it easier to test out the code!
5.  Commit your changes with a descriptive commit message.
6.  Push your branch to your fork: `git push origin your-feature-branch-name`
7.  Open a Pull Request to the `main` branch of the original `data-foundry` repository.
8.  The PR will be reviewed, and we might request changes.

## Styleguides

*   This is primarily a Java project. Please follow standard Java conventions.
*   For frontend code, we are increasingly using HTMX for interactivity.
*   Try to match the style of the surrounding code.

## License Agreement

By contributing, you agree that your contributions will be licensed under the **AGPL-3.0 License**.
