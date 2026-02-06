# EPANET Input File Format Reference

This document provides a comprehensive reference for the EPANET input file (.inp) format.

## Overview

EPANET input files are text-based configuration files that define a water distribution network model. The file is organized into sections, each beginning with a keyword enclosed in brackets (e.g., `[JUNCTIONS]`).

## File Structure

```
[TITLE]
...

[JUNCTIONS]
...

[RESERVOIRS]
...

[TANKS]
...

[PIPES]
...

[PUMPS]
...

[VALVES]
...

[END]
```

## Section Reference

### [TITLE]
Descriptive title for the network model.

**Format:** Free-form text (up to 3 lines)

```
[TITLE]
Example Network
A simple water distribution network model
```

### [JUNCTIONS]
Defines junction nodes where pipes connect.

**Format:**
```
;ID    Elevation    Demand    Pattern
```

| Field | Description | Units |
|-------|-------------|-------|
| ID | Junction identifier | - |
| Elevation | Elevation above datum | ft or m |
| Demand | Base demand | flow units |
| Pattern | Demand pattern ID (optional) | - |

**Example:**
```
[JUNCTIONS]
;ID    Elev    Demand    Pattern
J1     100     50        PAT1
J2     95      75        
```

### [RESERVOIRS]
Defines reservoir nodes (infinite water sources).

**Format:**
```
;ID    Head    Pattern
```

| Field | Description | Units |
|-------|-------------|-------|
| ID | Reservoir identifier | - |
| Head | Hydraulic head | ft or m |
| Pattern | Head pattern ID (optional) | - |

**Example:**
```
[RESERVOIRS]
;ID    Head    Pattern
R1     200     
```

### [TANKS]
Defines storage tank nodes.

**Format:**
```
;ID    Elevation    InitLevel    MinLevel    MaxLevel    Diameter    MinVol    VolCurve
```

| Field | Description | Units |
|-------|-------------|-------|
| ID | Tank identifier | - |
| Elevation | Bottom elevation | ft or m |
| InitLevel | Initial water level | ft or m |
| MinLevel | Minimum water level | ft or m |
| MaxLevel | Maximum water level | ft or m |
| Diameter | Tank diameter (cylindrical) | ft or m |
| MinVol | Minimum volume | volume units |
| VolCurve | Volume curve ID (optional) | - |

**Example:**
```
[TANKS]
;ID    Elev    InitLvl    MinLvl    MaxLvl    Diam    MinVol    VolCurve
T1     150     10         0         20        50      0         
```

### [PIPES]
Defines pipe links connecting nodes.

**Format:**
```
;ID    Node1    Node2    Length    Diameter    Roughness    MinorLoss    Status
```

| Field | Description | Units |
|-------|-------------|-------|
| ID | Pipe identifier | - |
| Node1 | Start node ID | - |
| Node2 | End node ID | - |
| Length | Pipe length | ft or m |
| Diameter | Pipe diameter | in or mm |
| Roughness | Roughness coefficient | depends on headloss formula |
| MinorLoss | Minor loss coefficient | - |
| Status | Initial status (Open/Closed/CV) | - |

**Example:**
```
[PIPES]
;ID    Node1    Node2    Length    Diam    Rough    MinorLoss    Status
P1     R1       J1       1000      12      100      0            Open
P2     J1       J2       500       10      100      0            Open
```

### [PUMPS]
Defines pump links.

**Format:**
```
;ID    Node1    Node2    Parameters
```

| Field | Description |
|-------|-------------|
| ID | Pump identifier |
| Node1 | Suction node ID |
| Node2 | Delivery node ID |
| Parameters | Pump curve definition |

**Parameters Options:**
- `HEAD curveID` - Uses pump curve
- `POWER value` - Constant power (hp or kW)
- `SPEED value` - Relative speed setting
- `PATTERN patternID` - Speed pattern

**Example:**
```
[PUMPS]
;ID    Node1    Node2    Parameters
PUMP1  R1       J1       HEAD 1
```

### [VALVES]
Defines valve links.

**Format:**
```
;ID    Node1    Node2    Diameter    Type    Setting    MinorLoss
```

| Field | Description |
|-------|-------------|
| ID | Valve identifier |
| Node1 | Upstream node ID |
| Node2 | Downstream node ID |
| Diameter | Valve diameter |
| Type | Valve type (PRV/PSV/PBV/FCV/TCV/GPV) |
| Setting | Pressure or flow setting |
| MinorLoss | Minor loss coefficient |

**Valve Types:**
- PRV: Pressure Reducing Valve
- PSV: Pressure Sustaining Valve
- PBV: Pressure Breaker Valve
- FCV: Flow Control Valve
- TCV: Throttle Control Valve
- GPV: General Purpose Valve

**Example:**
```
[VALVES]
;ID    Node1    Node2    Diam    Type    Setting    MinorLoss
V1     J1       J2       12      PRV     50         0
```

### [PATTERNS]
Defines time patterns for demands, heads, or other time-varying quantities.

**Format:**
```
;ID    Multipliers...
```

Multiple lines with same ID are concatenated.

**Example:**
```
[PATTERNS]
;ID    Multipliers
PAT1   1.0  1.2  1.4  1.6  1.4  1.2
PAT1   1.0  0.8  0.6  0.4  0.6  0.8
```

### [CURVES]
Defines data curves for pumps, efficiency, volumes, etc.

**Format:**
```
;ID    X-Value    Y-Value
```

| Curve Type | X-Value | Y-Value |
|------------|---------|---------|
| PUMP | Flow | Head |
| EFFICIENCY | Flow | Efficiency (%) |
| VOLUME | Depth | Volume |
| HEADLOSS | Flow | Headloss |

**Example:**
```
[CURVES]
;ID    X-Value    Y-Value
;PUMP: Pump Curve
1      0          150
1      500        120
1      1000       80
```

### [CONTROLS]
Defines simple control rules.

**Format:**
```
LINK linkID status IF NODE nodeID ABOVE/BELOW value
LINK linkID status AT TIME time
LINK linkID status AT CLOCKTIME clocktime AM/PM
```

**Example:**
```
[CONTROLS]
LINK PUMP1 OPEN IF NODE T1 BELOW 10
LINK PUMP1 CLOSED IF NODE T1 ABOVE 20
```

### [RULES]
Defines rule-based controls (more complex than simple controls).

**Format:**
```
RULE ruleID
IF condition
THEN action
ELSE action
PRIORITY value
```

**Example:**
```
[RULES]
RULE 1
IF TANK T1 LEVEL BELOW 10
THEN PUMP PUMP1 STATUS IS OPEN
ELSE PUMP PUMP1 STATUS IS CLOSED
PRIORITY 1
```

### [ENERGY]
Defines energy analysis parameters.

**Format:**
```
Global Efficiency    value
Global Price         value
Demand Charge        value
Pump    pumpID    PRICE/PATTERN/EFFIC    value
```

**Example:**
```
[ENERGY]
Global Efficiency    75
Global Price         0.1
Demand Charge        0
```

### [QUALITY]
Defines initial water quality at nodes.

**Format:**
```
;Node    InitQual
```

**Example:**
```
[QUALITY]
;Node    InitQual
R1       1.0
J1       0.5
```

### [SOURCES]
Defines water quality sources.

**Format:**
```
;Node    Type    Quality    Pattern
```

| Type | Description |
|------|-------------|
| CONCEN | Concentration |
| MASS | Mass booster |
| FLOWPACED | Flow-paced booster |
| SETPOINT | Setpoint booster |

### [REACTIONS]
Defines reaction rate coefficients.

**Global Parameters:**
```
Order Bulk     value
Order Tank     value
Order Wall     value
Global Bulk    value
Global Wall    value
Limiting Potential    value
Roughness Correlation    value
```

**Pipe/Tank Specific:**
```
Bulk    pipeID    value
Wall    pipeID    value
Tank    tankID    value
```

### [TIMES]
Defines time-related parameters.

**Format:**
```
Duration              value
Hydraulic Timestep    value
Quality Timestep      value
Pattern Timestep      value
Pattern Start         value
Report Timestep       value
Report Start          value
Start ClockTime       value
Statistic             NONE/AVERAGED/MINIMUM/MAXIMUM/RANGE
```

**Example:**
```
[TIMES]
Duration           24:00
Hydraulic Timestep 1:00
Quality Timestep   0:05
Pattern Timestep   2:00
Report Timestep    1:00
```

### [OPTIONS]
Defines analysis options.

**Common Options:**
| Option | Description | Values |
|--------|-------------|--------|
| Units | Flow units | CFS/GPM/MGD/IMGD/AFD/LPS/LPM/MLD/CMH/CMD |
| Headloss | Headloss formula | H-W/D-W/C-M |
| Specific Gravity | Specific gravity | Default: 1.0 |
| Viscosity | Relative viscosity | Default: 1.0 |
| Trials | Max iterations | Default: 40 |
| Accuracy | Convergence criterion | Default: 0.001 |
| Unbalanced | Action if unbalanced | STOP/CONTINUE |
| Pattern | Default demand pattern | Pattern ID |
| Quality | Quality parameter | NONE/CHEMICAL/AGE/TRACE |

**Example:**
```
[OPTIONS]
Units              GPM
Headloss           H-W
Specific Gravity   1.0
Viscosity          1.0
Trials             40
Accuracy           0.001
```

### [COORDINATES]
Defines node coordinates for mapping.

**Format:**
```
;Node    X-Coord    Y-Coord
```

### [VERTICES]
Defines intermediate points on link paths.

**Format:**
```
;Link    X-Coord    Y-Coord
```

### [LABELS]
Defines map labels.

**Format:**
```
;X-Coord    Y-Coord    "Label Text"    AnchorNode
```

### [BACKDROP]
Defines map backdrop image settings.

**Format:**
```
DIMENSIONS    x1    y1    x2    y2
UNITS         NONE/FEET/METERS
FILE          filename
OFFSET        x    y
```

### [END]
Marks the end of the input file.

## Units Reference

### Flow Units

| Keyword | Description |
|---------|-------------|
| CFS | Cubic feet per second |
| GPM | Gallons per minute |
| MGD | Million gallons per day |
| IMGD | Imperial MGD |
| AFD | Acre-feet per day |
| LPS | Liters per second |
| LPM | Liters per minute |
| MLD | Million liters per day |
| CMH | Cubic meters per hour |
| CMD | Cubic meters per day |

### Headloss Formulas

| Formula | Roughness Coefficient |
|---------|----------------------|
| H-W | Hazen-Williams C factor |
| D-W | Darcy-Weisbach roughness (ft or m) |
| C-M | Chezy-Manning n |

## Comments

Lines beginning with semicolon (;) are treated as comments:
```
; This is a comment
[JUNCTIONS]
; Junction data follows
J1    100    50    ; Junction 1
```
