"""APSIM soil regressions against the shared, rebuilt MasterInput fixture."""
import json
import sqlite3

import pytest

from modfilegen.soil_repository import SoilDataRepository
from modfilegen.parameter_resolver import ParameterResolver

from modfilegen.Converter.ApsimConverter.apsimsoilconverter import ApsimSoilConverter
from modfilegen.Converter.ApsimConverter.apsimconverter import (
    create_apsim_simulation, find_soil_component, update_apsim_template,
)


@pytest.fixture
def connections(masterinput_copy, modelsdictionary_db):
    with sqlite3.connect(masterinput_copy) as mi, sqlite3.connect(':memory:') as md:
        with sqlite3.connect(modelsdictionary_db) as source:
            source.backup(md)
        md.execute('INSERT INTO Variables (model, [Table], Champ, Type, Default_Value_Datamill) VALUES (?, ?, ?, ?, ?)',
                   ('apsim', 'Organic', 'FOMCNRatio', 'double', '37'))
        md.commit()
        mi.execute('UPDATE Soil SET Ssat = 48')
        mi.commit()
        yield mi, md


def prefetched_services(connections, soil_id):
    mi, md = connections
    repository = SoilDataRepository(mi)
    repository.prefetch([soil_id])
    resolver = ParameterResolver(md, mi)
    resolver.prefetch('apsim', ApsimSoilConverter.PARAMETER_TABLES, [soil_id])
    return resolver, repository


def export(connections, tmp_path):
    mi, md = connections
    soil_id = mi.execute('SELECT IdSoil FROM Soil').fetchone()[0]
    path = tmp_path / 'Soil.apsimx'
    resolver, repository = prefetched_services(connections, soil_id)
    result = ApsimSoilConverter().export(resolver, repository, path, soil_id)
    assert result == str(path)
    return find_soil_component(json.loads(path.read_text()))


def test_layer_mapping_and_units(connections, tmp_path):
    mi, _ = connections
    # Force a missing runoff relationship: it must not hide a physical soil.
    mi.execute('UPDATE Soil SET RunoffType = NULL, Ssat = 48')
    mi.execute('UPDATE SoilLayers SET Ssat = 45 WHERE NumLayer = 1')
    soil = export(connections, tmp_path)
    physical = soil['Children'][0]
    assert physical['Thickness'] == [300, 1150]
    assert physical['BD'] == [1.2, 1.3]
    assert physical['LL15'] == pytest.approx([.116, .112])
    assert physical['DUL'] == pytest.approx([.302, .302])
    assert physical['SAT'] == [.45, .48]
    assert next(c for c in soil['Children'] if c['Name'] == 'Organic')['Carbon'] == [.36, .36]
    assert next(c for c in soil['Children'] if c['Name'] == 'Chemical')['PH'] == [5, 5]


def test_soil_fallback_without_layers(connections, tmp_path):
    mi, _ = connections
    mi.execute('DELETE FROM SoilLayers')
    soil = export(connections, tmp_path)
    physical = soil['Children'][0]
    assert physical['Thickness'] == [1750]
    assert physical['BD'] == [1.622]
    assert physical['SAT'] == [.48]


def test_defaults_and_explicit_overrides(connections, tmp_path):
    mi, md = connections
    for table, name, value in [('Physical', 'KS', 12), ('Physical', 'DUL', .4),
                               ('SoilWater', 'SWCON', .25)]:
        md.execute('INSERT INTO Variables (model, [Table], Champ, Type, Default_Value_Datamill) VALUES (?, ?, ?, ?, ?)',
                   ('apsim', table, name, 'double', value))
    soil_id = mi.execute('SELECT IdSoil FROM Soil').fetchone()[0]
    mi.execute('INSERT INTO SoilParameterOverrides VALUES (?, ?, ?, ?, ?)',
               (soil_id, 'apsim', 'Physical', 'KS', '35'))
    soil = export(connections, tmp_path)
    assert soil['Children'][0]['KS'] == [35, 35]
    # Measured DUL takes precedence over the dictionary default.
    assert soil['Children'][0]['DUL'] == [.302, .302]
    assert next(c for c in soil['Children'] if c['Name'] == 'SoilWater')['SWCON'] == [.25, .25]


def test_explicit_soil_id_and_embedding(connections, tmp_path):
    mi, md = connections
    sim_id = mi.execute('SELECT idsim FROM SimUnitList LIMIT 1').fetchone()[0]
    path = tmp_path / 'Soil.apsimx'
    soil_id = mi.execute('SELECT idsoil FROM SimUnitList WHERE idsim = ?', (sim_id,)).fetchone()[0]
    resolver, repository = prefetched_services(connections, soil_id)
    queries = []
    mi.set_trace_callback(queries.append)
    md.set_trace_callback(queries.append)
    try:
        ApsimSoilConverter().export(resolver, repository, path, soil_id)
    finally:
        mi.set_trace_callback(None)
        md.set_trace_callback(None)
    assert queries == []
    row = dict(idsim=sim_id, StartYear=2000, EndYear=2001)
    create_apsim_simulation(str(tmp_path), row, None, str(path), None)
    simulation = tmp_path / 'Simulation.apsimx'
    assert find_soil_component(json.loads(simulation.read_text())) is not None
    update_apsim_template(str(simulation), row, None, str(path), None)
    assert find_soil_component(json.loads(simulation.read_text())) is not None


def test_invalid_thickness_fails(connections, tmp_path):
    mi, _ = connections
    mi.execute('UPDATE SoilLayers SET Ldown = Lup WHERE NumLayer = 1')
    with pytest.raises(ValueError, match='thickness'):
        export(connections, tmp_path)


def test_chunk_reuses_soil_and_writes_each_simulation(connections, tmp_path, monkeypatch):
    from modfilegen.Converter.ApsimConverter import apsimconverter as orchestrator
    mi, md = connections
    mi_path = tmp_path / 'input.db'
    md_path = tmp_path / 'dictionary.db'
    with sqlite3.connect(mi_path) as copy:
        mi.backup(copy)
    with sqlite3.connect(md_path) as copy:
        md.backup(copy)
    soil_id = mi.execute('SELECT IdSoil FROM Soil').fetchone()[0]
    monkeypatch.setattr(orchestrator.apsimweatherconverter.ApsimWeatherConverter,
                        'export', lambda *args: '')
    monkeypatch.setattr(orchestrator.apsimmanagementconverter.ApsimManagementConverter,
                        'export', lambda *args, **kwargs: None)
    monkeypatch.setattr(orchestrator.apsiminitconverter.ApsimInitConverter,
                        'export', lambda *args, **kwargs: None)
    original = ApsimSoilConverter.export
    calls = []

    def tracked(self, *args, **kwargs):
        calls.append(kwargs['id_soil'])
        return original(self, *args, **kwargs)

    monkeypatch.setattr(ApsimSoilConverter, 'export', tracked)
    rows = [dict(idsim=name, idPoint='point', StartYear=2000, EndYear=2001,
                 idsoil=soil_id, idMangt='management') for name in ('first', 'second')]
    orchestrator.process_chunk(rows, str(mi_path), str(md_path), str(tmp_path), None, 0, None)
    assert calls == [soil_id]
    assert (tmp_path / 'first' / 'Soil.apsimx').read_text() == (tmp_path / 'second' / 'Soil.apsimx').read_text()
    for name in ('first', 'second'):
        assert find_soil_component(json.loads((tmp_path / name / 'Simulation.apsimx').read_text()))


@pytest.mark.parametrize('configured', [None, .46])
def test_missing_saturation_requires_configuration(connections, tmp_path, configured):
    mi, md = connections
    mi.execute('UPDATE Soil SET Ssat = NULL')
    mi.execute('UPDATE SoilLayers SET Ssat = NULL')
    if configured is None:
        with pytest.raises(ValueError, match='Missing Ssat'):
            export(connections, tmp_path)
        assert not (tmp_path / 'Soil.apsimx').exists()
    else:
        md.execute('INSERT INTO Variables (model, [Table], Champ, Type, Default_Value_Datamill) VALUES (?, ?, ?, ?, ?)',
                   ('apsim', 'Physical', 'SAT', 'double', configured))
        assert export(connections, tmp_path)['Children'][0]['SAT'] == [configured, configured]


def test_particle_sizes_use_layers_then_soil(connections, tmp_path):
    mi, _ = connections
    # Existing fixture has Clay/Silt layers, but no Sand column.
    physical = export(connections, tmp_path)['Children'][0]
    assert physical['ParticleSizeSand'] == [51.725, 51.725]
    assert physical['ParticleSizeSilt'] == [12.0, 1.2]
    assert physical['ParticleSizeClay'] == [23.0, 12.0]


@pytest.mark.parametrize('missing', [None, '', -99])
def test_particle_sizes_missing_layer_values_fall_back(connections, tmp_path, missing):
    mi, _ = connections
    # Test-only migration on masterinput_copy: emulate the planned Sand column.
    # No versioned fixture database or text source is changed.
    mi.execute('ALTER TABLE SoilLayers ADD COLUMN Sand REAL')
    mi.execute('UPDATE SoilLayers SET Sand = 40 WHERE NumLayer = 1')
    mi.execute('UPDATE SoilLayers SET Sand = ?, Silt = ?, Clay = ? WHERE NumLayer = 2',
               (missing, missing, missing))
    physical = export(connections, tmp_path)['Children'][0]
    assert physical['ParticleSizeSand'] == [40, 51.725]
    assert physical['ParticleSizeSilt'] == [12.0, 13.55]
    assert physical['ParticleSizeClay'] == [23.0, 34.725]


def test_rocks_and_texture_mapping(connections, tmp_path):
    mi, _ = connections
    physical = export(connections, tmp_path)['Children'][0]
    assert physical['Rocks'] == [.5, 2.5]
    assert physical['Texture'] == ['sandy clay loam', 'sandy clay loam']
    mi.execute('UPDATE SoilLayers SET cf = NULL WHERE NumLayer = 2')
    physical = export(connections, tmp_path)['Children'][0]
    assert physical['Rocks'] == [.5, 7]


def test_texture_override_preserves_text(connections, tmp_path):
    mi, md = connections
    md.execute('INSERT INTO Variables (model, [Table], Champ, Type, Default_Value_Datamill) VALUES (?, ?, ?, ?, ?)',
               ('apsim', 'Physical', 'Texture', 'string', 'clay'))
    # Measured texture precedes the model default.
    assert export(connections, tmp_path)['Children'][0]['Texture'] == ['sandy clay loam'] * 2
    soil_id = mi.execute('SELECT IdSoil FROM Soil').fetchone()[0]
    mi.execute('INSERT INTO SoilParameterOverrides VALUES (?, ?, ?, ?, ?)',
               (soil_id, 'apsim', 'Physical', 'Texture', 'loam'))
    assert export(connections, tmp_path)['Children'][0]['Texture'] == ['loam'] * 2


def test_soil_cn_ratio_uses_soil_concentrations(connections, tmp_path):
    mi, _ = connections
    mi.execute('UPDATE Soil SET OrganicC = 1.2, OrganicNStock = 0.1')
    # Layer carbon remains distinct; the C/N ratio uses the matching Soil pair.
    organic = next(c for c in export(connections, tmp_path)['Children']
                   if c['Name'] == 'Organic')
    assert organic['Carbon'] == [.36, .36]
    assert organic['SoilCNRatio'] == pytest.approx([12, 12])


def test_soil_cn_ratio_beats_default_and_accepts_override(connections, tmp_path):
    mi, md = connections
    mi.execute('UPDATE Soil SET OrganicC = 1.5, OrganicNStock = 0.1')
    md.execute('INSERT INTO Variables (model, [Table], Champ, Type, Default_Value_Datamill) VALUES (?, ?, ?, ?, ?)',
               ('apsim', 'Organic', 'SoilCNRatio', 'double', '10'))
    def ratios():
        return next(c for c in export(connections, tmp_path)['Children']
                    if c['Name'] == 'Organic')['SoilCNRatio']
    assert ratios() == pytest.approx([15, 15])
    soil_id = mi.execute('SELECT IdSoil FROM Soil').fetchone()[0]
    mi.execute('INSERT INTO SoilParameterOverrides VALUES (?, ?, ?, ?, ?)',
               (soil_id, 'apsim', 'Organic', 'SoilCNRatio', '18'))
    assert ratios() == [18, 18]


@pytest.mark.parametrize('nitrogen', [None, 0, -99])
def test_missing_soil_nitrogen_requires_configured_ratio(connections, tmp_path, nitrogen):
    mi, md = connections
    mi.execute('UPDATE Soil SET OrganicNStock = ?', (nitrogen,))
    with pytest.raises(ValueError, match='Cannot determine SoilCNRatio'):
        export(connections, tmp_path)
    md.execute('INSERT INTO Variables (model, [Table], Champ, Type, Default_Value_Datamill) VALUES (?, ?, ?, ?, ?)',
               ('apsim', 'Organic', 'SoilCNRatio', 'double', '14'))
    organic = next(c for c in export(connections, tmp_path)['Children']
                   if c['Name'] == 'Organic')
    assert organic['SoilCNRatio'] == [14, 14]


def test_fom_cn_ratio_uses_resolver_default_and_soil_override(connections, tmp_path):
    mi, _ = connections
    def ratio():
        return next(c for c in export(connections, tmp_path)['Children']
                    if c['Name'] == 'Organic')['FOMCNRatio']
    assert ratio() == 37
    soil_id = mi.execute('SELECT IdSoil FROM Soil').fetchone()[0]
    mi.execute('INSERT INTO SoilParameterOverrides VALUES (?, ?, ?, ?, ?)',
               (soil_id, 'apsim', 'Organic', 'FOMCNRatio', '52'))
    assert ratio() == 52


def test_missing_fom_cn_ratio_fails_without_fixed_fallback(connections, tmp_path):
    _, md = connections
    md.execute("DELETE FROM Variables WHERE lower(model) = 'apsim' AND lower([Table]) = 'organic' AND lower(Champ) = 'fomcnratio'")
    with pytest.raises(ValueError, match='Missing Organic.FOMCNRatio'):
        export(connections, tmp_path)
    assert not (tmp_path / 'Soil.apsimx').exists()
