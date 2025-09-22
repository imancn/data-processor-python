# Change Logs

This directory contains detailed release notes and change history for the Data Processing Framework.

## File Naming Convention

Files are named using the pattern: `{number}_{version}_{description}.md`

- **Number**: Sequential number (001, 002, 003, ...)
- **Version**: Semantic version (v1.0.0, v2.0.0, ...)
- **Description**: Brief description of the release

## Available Releases

### [001_v2_0_0_architecture_refactoring.md](001_v2_0_0_architecture_refactoring.md)
**Release Date:** 2024-09-22  
**Type:** Major Release  
**Breaking Changes:** Yes

Major architecture refactoring introducing class-based pipeline system, generic utilities, and centralized management.

## Adding New Release Notes

When creating a new release:

1. Create a new file following the naming convention
2. Include release date, type, and breaking changes status
3. Document all significant changes
4. Include migration notes if applicable
5. Update this README with the new release

## Release Types

- **Major**: Breaking changes, significant new features
- **Minor**: New features, backward compatible
- **Patch**: Bug fixes, minor improvements
- **Hotfix**: Critical bug fixes
