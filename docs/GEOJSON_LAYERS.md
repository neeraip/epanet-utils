# EPANET GeoJSON layer schema

`epanet_utils.exports.emit_geojson_layers(inp_path, crs)` is the
canonical EPANET `.inp` → spatial-layer parser. It returns one layer
spec per HydraulicModel role; each spec wraps a GeoJSON
`FeatureCollection` whose feature properties follow the schema below.

This document is the contract the producer (this lib) and the consumers
(NEER's lambda-importer, Console attribute table, results-coloring
pipeline) agree on. Adding a property is non-breaking; renaming or
removing one is.

## Layer specs returned

```python
[
  {
    "name": str,             # capitalized layer name ("Junctions")
    "role": str,             # HydraulicModelRole token ("junction")
    "geometry_type": str,    # "Point" | "LineString"
    "crs": str | None,       # caller-supplied (e.g. "EPSG:4326")
    "feature_collection": {
      "type": "FeatureCollection",
      "features": [...],
    },
  },
  ...
]
```

`role` ↔ `name` mapping (`LAYER_ROLE_MAP`):

| name | role | geometry |
|---|---|---|
| Junctions | junction | Point |
| Reservoirs | reservoir | Point |
| Tanks | tank | Point |
| Pipes | pipe | LineString |
| Pumps | pump | LineString |
| Valves | valve | LineString |

Empty roles are omitted from the returned list.

## Feature `id`

Each feature's top-level `id` is the EPANET element id (the `id`
column of the source section). MapLibre `promoteId="id"` (or `"name"` —
both populated in feature properties) lets feature-state attached by
element id flow through PMTiles.

## Properties — by role

Every feature's `properties` dict is the source-section row plus any
applicable cross-references. Source-section column names match
`EpanetInputDecoder.SECTION_COLUMNS` exactly (lowercase, snake_case
where multi-word). Cross-reference keys carry no prefix when they are
single-valued (`tag`, `initial_quality`, `initial_status`,
`emitter_coefficient`); multi-field summaries use a stable prefix
(`source_*`, `mixing_*`, `param_*`, `parameters_kind`).

### junction

```jsonc
{
  "id":        "J1",
  "elevation": 100,         // [JUNCTIONS]
  "demand":    25,
  "pattern":   "DailyPat",  // optional

  "tag":             "...",       // [TAGS]
  "initial_quality": 0.5,         // [QUALITY]
  "emitter_coefficient": 0.1,     // [EMITTERS]

  // [DEMANDS] — multi-row (one per category) collapsed to count + sum.
  "demand_count": 3,
  "total_base_demand": 75.0,

  // [SOURCES] — single row per node.
  "source_type":    "MASS",
  "source_quality": 1.0,
  "source_pattern": "ChlorPat",
}
```

### reservoir

```jsonc
{
  "id": "R1",
  "head":    220.0,        // [RESERVOIRS]
  "pattern": "...",        // optional

  "tag":             "...",
  "initial_quality": 0.0,
  // sources/demands/emitter cross-refs do not apply to reservoirs
}
```

### tank

```jsonc
{
  "id": "T1",
  "elevation":  150.0,     // [TANKS]
  "init_level": 5.0,
  "min_level":  2.0,
  "max_level":  20.0,
  "diameter":   30.0,
  "min_vol":    0,
  "vol_curve":  "...",     // optional

  "tag":             "...",
  "initial_quality": 0.0,
  "demand_count":    0,
  "total_base_demand": 0.0,
  "source_type":     "...",
  "source_quality":  0.0,
  "source_pattern":  "...",

  // [MIXING] — tanks only.
  "mixing_model":    "MIXED",
  "mixing_fraction": 0.5,
}
```

### pipe

```jsonc
{
  "id": "P1",
  "node1":      "J1",      // [PIPES]
  "node2":      "J2",
  "length":     1000,
  "diameter":   12,
  "roughness":  100,
  "minor_loss": 0,
  "status":     "Open",    // declared default

  "tag":            "...",
  "initial_status": "OPEN", // [STATUS] override (uppercased; null if absent)
}
```

### pump

EPANET pumps' opaque `parameters` blob is parsed into structured
columns + a kind summary so consumers don't have to reparse:

```jsonc
{
  "id":         "Pmp1",
  "node1":      "J1",
  "node2":      "T1",
  "parameters": "HEAD curve1 SPEED 1.2",   // verbatim from .inp

  "tag":            "...",
  "initial_status": "OPEN",

  // Expanded from `parameters`:
  "param_head":    "curve1",
  "param_speed":   "1.2",
  "param_pattern": "...",   // when PATTERN <id>
  "param_price":   "0.05",  // when PRICE <v>
  "param_effic":   "...",   // when EFFIC <id>
  "param_power":   "50",    // when POWER <v>
  "parameters_kind": "HEAD,SPEED",  // ordered, comma-joined
}
```

Recognized keywords: `HEAD`, `POWER`, `SPEED`, `PATTERN`, `PRICE`,
`EFFIC`. Unknown tokens are skipped (they remain in the verbatim
`parameters` string).

### valve

```jsonc
{
  "id": "V1",
  "node1":      "J1",       // [VALVES]
  "node2":      "J2",
  "diameter":   12,
  "type":       "PRV",
  "setting":    100,
  "minor_loss": 0,

  "tag":            "...",
  "initial_status": "ACTIVE",
}
```

## Stability guarantees

- Property names are stable. Adding a new property is non-breaking;
  renaming or removing one is breaking.
- Cross-reference prefixes (`source_`, `mixing_`, `param_`) are stable.
- Source-section values pass through `EpanetInputDecoder._convert_value`
  (int → float → string fallback), so numeric columns arrive as
  numbers, not strings. Cross-reference numeric summaries
  (`emitter_coefficient`, `total_base_demand`, `mixing_fraction`,
  `initial_quality`) are emitted as `float | None` via `_sf()`.
- Geometry coordinate values are passed through verbatim from
  `[COORDINATES]` / `[VERTICES]` — no reprojection happens here.
  The caller is responsible for honoring `crs`.
