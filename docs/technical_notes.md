# Region Mapping Technical Note

- pa_loca behaves as an opaque locality code family (mostly 3-char alphanumeric tokens).
- No valid crosswalk was found for most pa_loca codes against dim_region or organisme location fields.
- Strict region mapping coverage is 0.432233% (3244/750521).
- codreg=000 fallback mapping was rejected as semantically invalid.
- Therefore, unmatched rows are treated as Unknown region (dw_region_key=0) in DW-safe outputs.
