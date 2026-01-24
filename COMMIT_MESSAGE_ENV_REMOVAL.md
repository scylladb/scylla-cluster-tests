# Commit Message

```
refactor(sct-config): remove unused env parameters from sct_field calls

Remove all env= parameters from sct_field() function calls throughout
the SCTConfiguration class. These environment variable name parameters
were no longer being used by the configuration system and added
unnecessary verbosity to the field definitions.

This cleanup simplifies the configuration field definitions by removing
approximately 420 lines of redundant env= parameter declarations while
preserving all other field attributes including description, default,
appendable, and choices parameters.
```

## Changes Summary

- Removed all `env="SCT_..."` parameters from `sct_field()` calls in `sdcm/sct_config.py`
- Reduced file from ~4186 lines to ~3763 lines
- No functional changes - only removal of unused parameters
- All field definitions retain their description, default values, and other attributes

## Files Changed

- `sdcm/sct_config.py` - Removed env parameters from all sct_field() calls
