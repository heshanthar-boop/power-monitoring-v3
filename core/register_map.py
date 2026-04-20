# core/register_map.py
"""
Selec MFM384-C Complete Register Map
- FC04 Input Registers (30000+) - Float32, 2 registers each
- FC03 Holding Registers (40000+) - Integer, configuration parameters
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import ClassVar, Tuple, List, Dict, Optional, Union


@dataclass(frozen=True)
class RegisterDefinition:
    """FC04 Input Register definition (read-only measured values)"""
    name: str
    offset: int          # FC04 offset from base (30000+offset)
    unit: str
    desc: str


@dataclass(frozen=True)
class SetupRegisterDef:
    """FC03 Holding Register definition (400xx setup parameters)"""
    name: str
    offset: int          # 40000 + offset
    nregs: int           # number of registers (1 or 2)
    dtype: str           # 'int', 'uint16', 'uint32', 'enum', 'scaled'
    min_val: Optional[int] = None
    max_val: Optional[int] = None
    enum_map: Optional[Dict[int, str]] = None
    scale: Optional[float] = None  # for scaled values like pulse weight/duration
    desc: str = ""
    write_only: bool = False


class MFM384RegisterMap:
    """
    Selec MFM384-C Complete Register Map
    
    FC04 Input Registers (base 30000):
    - Measured values as Float32 (2 registers each)
    - Word order default: CDAB (Mid Little Endian)
    
    FC03 Holding Registers (base 40000):
    - Configuration parameters as integers
    - Some have enum mappings, some are scaled, some are write-only reset commands
    """

    # =========================================================================
    # FC04 INPUT REGISTERS (30000+) - Float32 values, 2 registers each
    # =========================================================================
    INPUT_REGISTERS: ClassVar[Tuple[RegisterDefinition, ...]] = (
        # Voltage L-N
        RegisterDefinition("Voltage V1N", 0, "V", "Phase 1 to Neutral voltage"),
        RegisterDefinition("Voltage V2N", 2, "V", "Phase 2 to Neutral voltage"),
        RegisterDefinition("Voltage V3N", 4, "V", "Phase 3 to Neutral voltage"),
        RegisterDefinition("Average Voltage LN", 6, "V", "Average Line to Neutral voltage"),
        
        # Voltage L-L
        RegisterDefinition("Voltage V12", 8, "V", "Phase 1 to Phase 2 voltage"),
        RegisterDefinition("Voltage V23", 10, "V", "Phase 2 to Phase 3 voltage"),
        RegisterDefinition("Voltage V31", 12, "V", "Phase 3 to Phase 1 voltage"),
        RegisterDefinition("Average Voltage LL", 14, "V", "Average Line to Line voltage"),

        # Current
        RegisterDefinition("Current I1", 16, "A", "Phase 1 current"),
        RegisterDefinition("Current I2", 18, "A", "Phase 2 current"),
        RegisterDefinition("Current I3", 20, "A", "Phase 3 current"),
        RegisterDefinition("Average Current", 22, "A", "Average 3-phase current"),

        # Active Power (kW)
        RegisterDefinition("kW1", 24, "kW", "Phase 1 active power"),
        RegisterDefinition("kW2", 26, "kW", "Phase 2 active power"),
        RegisterDefinition("kW3", 28, "kW", "Phase 3 active power"),

        # Apparent Power (kVA)
        RegisterDefinition("kVA1", 30, "kVA", "Phase 1 apparent power"),
        RegisterDefinition("kVA2", 32, "kVA", "Phase 2 apparent power"),
        RegisterDefinition("kVA3", 34, "kVA", "Phase 3 apparent power"),

        # Reactive Power (kVAr)
        RegisterDefinition("kVAr1", 36, "kVAr", "Phase 1 reactive power"),
        RegisterDefinition("kVAr2", 38, "kVAr", "Phase 2 reactive power"),
        RegisterDefinition("kVAr3", 40, "kVAr", "Phase 3 reactive power"),

        # Total Power
        RegisterDefinition("Total kW", 42, "kW", "Total 3-phase active power"),
        RegisterDefinition("Total kVA", 44, "kVA", "Total 3-phase apparent power"),
        RegisterDefinition("Total kVAr", 46, "kVAr", "Total 3-phase reactive power"),

        # Power Factor
        RegisterDefinition("PF1", 48, "", "Phase 1 power factor"),
        RegisterDefinition("PF2", 50, "", "Phase 2 power factor"),
        RegisterDefinition("PF3", 52, "", "Phase 3 power factor"),
        RegisterDefinition("Average PF", 54, "", "Average 3-phase power factor"),

        # Frequency
        RegisterDefinition("Frequency", 56, "Hz", "System frequency"),

        # Net Energy
        RegisterDefinition("Total Net kWh", 58, "kWh", "Net active energy"),
        RegisterDefinition("Total Net kVAh", 60, "kVAh", "Net apparent energy"),
        RegisterDefinition("Total Net kVArh", 62, "kVArh", "Net reactive energy"),

        # Power Demand Max/Min
        RegisterDefinition("kW Active Power Max DMD", 64, "kW", "Maximum active power demand"),
        RegisterDefinition("kW Active Power Min DMD", 66, "kW", "Minimum active power demand"),
        RegisterDefinition("kVAr Reactive Power Max DMD", 68, "kVAr", "Maximum reactive power demand"),
        RegisterDefinition("kVAr Reactive Power Min DMD", 70, "kVAr", "Minimum reactive power demand"),
        RegisterDefinition("kVA Apparent Power Max DMD", 72, "kVA", "Maximum apparent power demand"),

        # Auxiliary and Run Hour
        RegisterDefinition("Auxiliary Interrupt", 80, "", "Auxiliary interrupt counter"),
        RegisterDefinition("Run hour", 82, "h", "Equipment run time hours"),

        # Phase Import kWh
        RegisterDefinition("kWh1 Import", 84, "kWh", "Phase 1 import active energy"),
        RegisterDefinition("kWh2 Import", 86, "kWh", "Phase 2 import active energy"),
        RegisterDefinition("kWh3 Import", 88, "kWh", "Phase 3 import active energy"),

        # Phase Export kWh
        RegisterDefinition("kWh1 Export", 90, "kWh", "Phase 1 export active energy"),
        RegisterDefinition("kWh2 Export", 92, "kWh", "Phase 2 export active energy"),
        RegisterDefinition("kWh3 Export", 94, "kWh", "Phase 3 export active energy"),

        # Total Import/Export kWh
        RegisterDefinition("Total kWh Import", 96, "kWh", "Total import active energy"),
        RegisterDefinition("Total kWh Export", 98, "kWh", "Total export active energy"),

        # Phase Import kVArh
        RegisterDefinition("kVArh1 Import", 100, "kVArh", "Phase 1 import reactive energy"),
        RegisterDefinition("kVArh2 Import", 102, "kVArh", "Phase 2 import reactive energy"),
        RegisterDefinition("kVArh3 Import", 104, "kVArh", "Phase 3 import reactive energy"),

        # Phase Export kVArh
        RegisterDefinition("kVArh1 Export", 106, "kVArh", "Phase 1 export reactive energy"),
        RegisterDefinition("kVArh2 Export", 108, "kVArh", "Phase 2 export reactive energy"),
        RegisterDefinition("kVArh3 Export", 110, "kVArh", "Phase 3 export reactive energy"),

        # Total Import/Export kVArh
        RegisterDefinition("Total kVArh Import", 112, "kVArh", "Total import reactive energy"),
        RegisterDefinition("Total kVArh Export", 114, "kVArh", "Total export reactive energy"),

        # Phase kVAh
        RegisterDefinition("kVAh1", 116, "kVAh", "Phase 1 apparent energy"),
        RegisterDefinition("kVAh2", 118, "kVAh", "Phase 2 apparent energy"),
        RegisterDefinition("kVAh3", 120, "kVAh", "Phase 3 apparent energy"),

        # Neutral Current
        RegisterDefinition("Neutral Current", 122, "A", "Neutral current"),

        # THD Voltage
        RegisterDefinition("THD Voltage V1N", 124, "%", "THD of 1st Phase Voltage"),
        RegisterDefinition("THD Voltage V2N", 126, "%", "THD of 2nd Phase Voltage"),
        RegisterDefinition("THD Voltage V3N", 128, "%", "THD of 3rd Phase Voltage"),
        RegisterDefinition("THD Voltage V12", 130, "%", "THD of Voltage V12"),
        RegisterDefinition("THD Voltage V23", 132, "%", "THD of Voltage V23"),
        RegisterDefinition("THD Voltage V31", 134, "%", "THD of Voltage V31"),

        # THD Current
        RegisterDefinition("THD Current I1", 136, "%", "THD of Current I1"),
        RegisterDefinition("THD Current I2", 138, "%", "THD of Current I2"),
        RegisterDefinition("THD Current I3", 140, "%", "THD of Current I3"),
    )

    # =========================================================================
    # FC04 INDIVIDUAL HARMONICS (30000+) - Float32 values, 2 registers each
    # =========================================================================
    #
    # Address formula from the Selec MFM384 harmonic register table:
    #   offset = 143 + ((harmonic_no - 2) * 2) + (60 * constant_parameter)
    #   register = 30000 + offset
    #
    # Example:
    #   14th harmonic of Voltage L3-L1 = 143 + ((14 - 2) * 2) + (60 * 5)
    #                                  = offset 467, register 30467
    HARMONIC_BASE_OFFSET = 143
    HARMONIC_STRIDE = 2
    HARMONIC_PARAM_STRIDE = 60
    HARMONIC_MIN = 2
    HARMONIC_MAX = 31
    HARMONIC_PARAMETERS: ClassVar[Dict[str, Tuple[int, str]]] = {
        "V1N": (0, "Voltage L1-N"),
        "V2N": (1, "Voltage L2-N"),
        "V3N": (2, "Voltage L3-N"),
        "V12": (3, "Voltage L1-L2"),
        "V23": (4, "Voltage L2-L3"),
        "V31": (5, "Voltage L3-L1"),
        "I1": (6, "Current L1"),
        "I2": (7, "Current L2"),
        "I3": (8, "Current L3"),
    }
    HARMONIC_PARAMETER_ALIASES: ClassVar[Dict[str, str]] = {
        "0": "V1N",
        "1": "V2N",
        "2": "V3N",
        "3": "V12",
        "4": "V23",
        "5": "V31",
        "6": "I1",
        "7": "I2",
        "8": "I3",
        "V1N": "V1N",
        "V2N": "V2N",
        "V3N": "V3N",
        "V12": "V12",
        "V23": "V23",
        "V31": "V31",
        "I1": "I1",
        "I2": "I2",
        "I3": "I3",
        "VL1N": "V1N",
        "VL2N": "V2N",
        "VL3N": "V3N",
        "VL1L2": "V12",
        "VL2L3": "V23",
        "VL3L1": "V31",
        "VOLTAGEL1N": "V1N",
        "VOLTAGEL2N": "V2N",
        "VOLTAGEL3N": "V3N",
        "VOLTAGEL1L2": "V12",
        "VOLTAGEL2L3": "V23",
        "VOLTAGEL3L1": "V31",
        "CURRENTL1": "I1",
        "CURRENTL2": "I2",
        "CURRENTL3": "I3",
    }

    @classmethod
    def harmonic_parameter(cls, parameter: Union[str, int]) -> Tuple[str, int, str]:
        """Return (short_key, constant_parameter, label) for a harmonic channel."""
        if isinstance(parameter, int):
            key = cls.HARMONIC_PARAMETER_ALIASES.get(str(parameter))
        else:
            normalized = "".join(ch for ch in str(parameter).upper() if ch.isalnum())
            key = cls.HARMONIC_PARAMETER_ALIASES.get(normalized)
        if not key or key not in cls.HARMONIC_PARAMETERS:
            raise ValueError(f"Unsupported harmonic parameter: {parameter!r}")
        constant, label = cls.HARMONIC_PARAMETERS[key]
        return key, constant, label

    @classmethod
    def harmonic_offset(cls, harmonic_no: int, parameter: Union[str, int]) -> int:
        """Return the zero-based FC04 offset for an individual harmonic value."""
        harmonic = int(harmonic_no)
        if harmonic < cls.HARMONIC_MIN or harmonic > cls.HARMONIC_MAX:
            raise ValueError(
                f"Harmonic number must be {cls.HARMONIC_MIN}-{cls.HARMONIC_MAX}, got {harmonic_no!r}"
            )
        _, constant, _ = cls.harmonic_parameter(parameter)
        return (
            cls.HARMONIC_BASE_OFFSET
            + ((harmonic - cls.HARMONIC_MIN) * cls.HARMONIC_STRIDE)
            + (cls.HARMONIC_PARAM_STRIDE * constant)
        )

    @classmethod
    def harmonic_register(cls, harmonic_no: int, parameter: Union[str, int]) -> int:
        """Return the displayed 3xxxx register number for an individual harmonic."""
        return 30000 + cls.harmonic_offset(harmonic_no, parameter)

    # =========================================================================
    # FC03 HOLDING REGISTERS (40000+) - Setup/Configuration Parameters
    # =========================================================================
    
    # Enum mappings for setup registers
    NETWORK_SELECTION_MAP = {0: "3P4W", 1: "3P3W", 2: "1P2W-P1", 3: "1P2W-P2", 4: "1P2W-P3"}
    BAUD_RATE_MAP = {0: "300", 1: "600", 2: "1200", 3: "2400", 4: "4800", 5: "9600", 6: "19200"}
    PARITY_MAP = {0: "None", 1: "Odd", 2: "Even"}
    STOP_BIT_MAP = {0: "1", 1: "2"}
    DEMAND_METHOD_MAP = {0: "Sliding", 1: "Fixed"}
    ENDIANNESS_MAP = {0: "CDAB (Mid Little Endian)", 1: "ABCD (Big Endian)"}
    
    SETUP_400XX_DEFS: ClassVar[Tuple[SetupRegisterDef, ...]] = (
        # Basic device configuration
        SetupRegisterDef("Password", 0, 1, "int", 0, 9998, None, None, "Device password (0-9998)"),
        SetupRegisterDef("Network Selection", 1, 1, "enum", 0, 4, 
            {0: "3P4W", 1: "3P3W", 2: "1P2W-P1", 3: "1P2W-P2", 4: "1P2W-P3"},
            None, "Network configuration"),
        SetupRegisterDef("CT Secondary", 2, 1, "int", 1, 5, None, None, "CT secondary (1 or 5)"),
        SetupRegisterDef("CT Primary", 3, 1, "int", 1, 10000, None, None, "CT primary value"),
        SetupRegisterDef("PT Secondary", 4, 1, "int", 100, 500, None, None, "PT secondary voltage"),
        SetupRegisterDef("PT Primary", 5, 2, "uint32", 100, 500000, None, None, "PT primary (100-500kV, 2 regs)"),
        
        # Communication parameters
        SetupRegisterDef("Slave ID", 7, 1, "int", 1, 255, None, None, "Modbus slave ID"),
        SetupRegisterDef("Baud Rate", 8, 1, "enum", 0, 6,
            {0: "300", 1: "600", 2: "1200", 3: "2400", 4: "4800", 5: "9600", 6: "19200"},
            None, "Serial baud rate"),
        SetupRegisterDef("Parity", 9, 1, "enum", 0, 2,
            {0: "None", 1: "Odd", 2: "Even"},
            None, "Serial parity"),
        SetupRegisterDef("Stop Bit", 10, 1, "enum", 0, 1,
            {0: "1", 1: "2"},
            None, "Serial stop bits"),
        
        # Display settings
        SetupRegisterDef("Backlight OFF Time", 11, 1, "int", 0, 7200, None, None, "Backlight timeout (0=always ON)"),
        
        # Factory defaults (write-only)
        SetupRegisterDef("Factory Default", 12, 1, "int", 0, 1, None, None, "Write 1 to factory reset", write_only=True),
        
        # Energy reset commands (write-only)
        SetupRegisterDef("Reset Active Energy", 13, 1, "int", 0, 1, None, None, "Write 1 to reset", write_only=True),
        SetupRegisterDef("Reset Apparent Energy", 14, 1, "int", 0, 1, None, None, "Write 1 to reset", write_only=True),
        SetupRegisterDef("Reset Reactive Energy", 15, 1, "int", 0, 1, None, None, "Write 1 to reset", write_only=True),
        
        # Auto mode pages
        SetupRegisterDef("Auto Mode Pages", 16, 1, "int", 1, 21, None, None, "Max auto-display pages (1-21)"),
        
        # Page address sequence (40017-40033, 40054-40055, 40059-40060)
        SetupRegisterDef("Page Seq 1", 17, 1, "int", 1, 21, None, None, "Page sequence slot 1"),
        SetupRegisterDef("Page Seq 2", 18, 1, "int", 1, 21, None, None, "Page sequence slot 2"),
        SetupRegisterDef("Page Seq 3", 19, 1, "int", 1, 21, None, None, "Page sequence slot 3"),
        SetupRegisterDef("Page Seq 4", 20, 1, "int", 1, 21, None, None, "Page sequence slot 4"),
        SetupRegisterDef("Page Seq 5", 21, 1, "int", 1, 21, None, None, "Page sequence slot 5"),
        SetupRegisterDef("Page Seq 6", 22, 1, "int", 1, 21, None, None, "Page sequence slot 6"),
        SetupRegisterDef("Page Seq 7", 23, 1, "int", 1, 21, None, None, "Page sequence slot 7"),
        SetupRegisterDef("Page Seq 8", 24, 1, "int", 1, 21, None, None, "Page sequence slot 8"),
        SetupRegisterDef("Page Seq 9", 25, 1, "int", 1, 21, None, None, "Page sequence slot 9"),
        SetupRegisterDef("Page Seq 10", 26, 1, "int", 1, 21, None, None, "Page sequence slot 10"),
        SetupRegisterDef("Page Seq 11", 27, 1, "int", 1, 21, None, None, "Page sequence slot 11"),
        SetupRegisterDef("Page Seq 12", 28, 1, "int", 1, 21, None, None, "Page sequence slot 12"),
        SetupRegisterDef("Page Seq 13", 29, 1, "int", 1, 21, None, None, "Page sequence slot 13"),
        SetupRegisterDef("Page Seq 14", 30, 1, "int", 1, 21, None, None, "Page sequence slot 14"),
        SetupRegisterDef("Page Seq 15", 31, 1, "int", 1, 21, None, None, "Page sequence slot 15"),
        SetupRegisterDef("Page Seq 16", 32, 1, "int", 1, 21, None, None, "Page sequence slot 16"),
        SetupRegisterDef("Page Seq 17", 33, 1, "int", 1, 21, None, None, "Page sequence slot 17"),
        
        # Demand settings
        SetupRegisterDef("Demand Interval Method", 34, 1, "enum", 0, 1,
            {0: "Sliding", 1: "Fixed"},
            None, "Demand calculation method"),
        SetupRegisterDef("Demand Interval Duration", 35, 1, "int", 1, 30, None, None, "Demand interval in minutes"),
        SetupRegisterDef("Demand Interval Length", 36, 1, "int", 1, 30, None, None, "Demand subperiod length"),
        
        # Reset commands (write-only)
        SetupRegisterDef("Reset Active Power Max DMD", 37, 1, "int", 0, 1, None, None, "Write 1 to reset", write_only=True),
        SetupRegisterDef("Reset Active Power Min DMD", 38, 1, "int", 0, 1, None, None, "Write 1 to reset", write_only=True),
        SetupRegisterDef("Reset Reactive Power Max DMD", 39, 1, "int", 0, 1, None, None, "Write 1 to reset", write_only=True),
        SetupRegisterDef("Reset Apparent Power Max DMD", 41, 1, "int", 0, 1, None, None, "Write 1 to reset", write_only=True),
        SetupRegisterDef("Reset Run Hour", 42, 1, "int", 0, 1, None, None, "Write 1 to reset", write_only=True),
        SetupRegisterDef("Reset Auxiliary Interrupt", 43, 1, "int", 0, 1, None, None, "Write 1 to reset", write_only=True),
        SetupRegisterDef("Reset Reactive Power Min DMD", 44, 1, "int", 0, 1, None, None, "Write 1 to reset", write_only=True),
        
        # More page sequences
        SetupRegisterDef("Page Seq 18", 54, 1, "int", 1, 21, None, None, "Page sequence slot 18"),
        SetupRegisterDef("Page Seq 19", 55, 1, "int", 1, 21, None, None, "Page sequence slot 19"),
        
        # Pulse settings (scaled values)
        SetupRegisterDef("Pulse Duration", 57, 1, "scaled", 1, 20, None, 10.0, "Pulse duration (0.1-2.0 sec, stored as 0.1s units)"),
        SetupRegisterDef("Pulse Weight", 58, 1, "scaled", 1, 9999, None, 100.0, "Pulse weight (0.01-99.99 kWh, stored as 0.01 units)"),
        
        # More page sequences
        SetupRegisterDef("Page Seq 20", 59, 1, "int", 1, 21, None, None, "Page sequence slot 20"),
        SetupRegisterDef("Page Seq 21", 60, 1, "int", 1, 21, None, None, "Page sequence slot 21"),
        
        # Reset max current
        SetupRegisterDef("Reset Max Current", 64, 1, "int", 0, 1, None, None, "Write 1 to reset", write_only=True),
        
        # Endianness
        SetupRegisterDef("Change Endianness", 70, 1, "enum", 0, 1,
            {0: "CDAB (Mid Little Endian)", 1: "ABCD (Big Endian)"},
            None, "Word order for float32"),
    )

    # Backward compatibility: simple tuple list
    SETUP_400XX: ClassVar[List[Tuple[str, int, int, str]]] = [
        (d.name, d.offset, d.nregs, d.desc) for d in SETUP_400XX_DEFS
    ]

    # Lookup tables for quick access
    SETUP_400XX_BY_OFFSET: ClassVar[Dict[int, SetupRegisterDef]] = {d.offset: d for d in SETUP_400XX_DEFS}
    SETUP_400XX_BY_NAME: ClassVar[Dict[str, SetupRegisterDef]] = {d.name: d for d in SETUP_400XX_DEFS}

    # Baud rate code mappings
    BAUD_RATE_CODES: ClassVar[Dict[int, int]] = {0: 300, 1: 600, 2: 1200, 3: 2400, 4: 4800, 5: 9600, 6: 19200}
    BAUD_RATE_TO_CODE: ClassVar[Dict[int, int]] = {v: k for k, v in BAUD_RATE_CODES.items()}

    # Parity code mappings
    PARITY_CODES: ClassVar[Dict[int, str]] = {0: "N", 1: "O", 2: "E"}
    PARITY_TO_CODE: ClassVar[Dict[str, int]] = {"N": 0, "O": 1, "E": 2}
    
    # Endianness code mappings
    ENDIANNESS_CODES: ClassVar[Dict[int, str]] = {0: "CDAB", 1: "ABCD"}
    ENDIANNESS_TO_CODE: ClassVar[Dict[str, int]] = {"CDAB": 0, "ABCD": 1}


# =============================================================================
# Module-level exports for backward compatibility and easy importing
# =============================================================================
SETUP_400XX = MFM384RegisterMap.SETUP_400XX
SETUP_400XX_DEFS = MFM384RegisterMap.SETUP_400XX_DEFS
SETUP_400XX_BY_OFFSET = MFM384RegisterMap.SETUP_400XX_BY_OFFSET
SETUP_400XX_BY_NAME = MFM384RegisterMap.SETUP_400XX_BY_NAME
BAUD_RATE_CODES = MFM384RegisterMap.BAUD_RATE_CODES
BAUD_RATE_TO_CODE = MFM384RegisterMap.BAUD_RATE_TO_CODE
PARITY_CODES = MFM384RegisterMap.PARITY_CODES
PARITY_TO_CODE = MFM384RegisterMap.PARITY_TO_CODE
ENDIANNESS_CODES = MFM384RegisterMap.ENDIANNESS_CODES
ENDIANNESS_TO_CODE = MFM384RegisterMap.ENDIANNESS_TO_CODE
