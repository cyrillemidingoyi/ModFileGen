# ApsimConverter module
# This module provides converters for APSIM model file generation

"""
APSIM Converter Module

Converts various data formats to APSIM Next Generation formats.

Components:
- ApsimWeatherConverter: Converts weather data to APSIM .met format
- ApsimSoilConverter: Converts soil data to APSIM .apsimx soil structures
- ApsimManagementConverter: Converts management operations to APSIM Manager scripts
- ApsimInitConverter: Converts initial conditions to APSIM initialization format
- ApsimConverter: Main orchestrator for complete APSIM workflow (export function)

Usage:
    from modfilegen.Converter.ApsimConverter import (
        ApsimWeatherConverter,
        ApsimSoilConverter,
        ApsimManagementConverter,
        ApsimInitConverter,
        export  # Main function
    )
    
    # Or use the main export function directly
    from modfilegen.Converter import ApsimConverter
    results = ApsimConverter.export(MasterInput, ModelDictionary, directoryPath)
"""

from .apsimweatherconverter import ApsimWeatherConverter
from .apsimsoilconverter import ApsimSoilConverter
from .apsimmanagementconverter import ApsimManagementConverter
from .apsiminitconverter import ApsimInitConverter
from .apsimconverter import export, main, process_chunk, transform_output, run_apsim

__all__ = [
    'ApsimWeatherConverter',
    'ApsimSoilConverter',
    'ApsimManagementConverter',
    'ApsimInitConverter',
    'export',
    'main',
    'process_chunk',
    'transform_output',
    'run_apsim'
]
