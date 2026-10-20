"""Build a shared agronomic APSIM NG dictionary without executing upstream code.

Requires tree-sitter and tree-sitter-c-sharp (generation dependencies only).
The input SQLite database is opened read-only, for column names only.
"""

import argparse
from collections import Counter
import csv
from datetime import date
import hashlib
import html
import json
from pathlib import Path
import re
import sqlite3
import subprocess
from urllib.parse import quote

from tree_sitter import Language, Parser
import tree_sitter_c_sharp


# Functional categories shared by crops; class names are provenance only.
CATEGORIES = {
    "Models.Soils.Physical": "physical",
    "Models.Soils.Organic": "organic",
    "Models.Soils.Chemical": "chemical",
    "Models.Soils.Soil": "soil",
    "Models.Soils.Water": "initialwater",
    "Models.Soils.SoilCrop": "soilcrop",
    "Models.Soils.Solute": "solute",
    "Models.WaterModel.WaterBalance": "soilwater",
    "Models.Soils.CERESSoilTemperature": "soiltemperature",
    "Models.Soils.SoilTemp.SoilTemperature": "soiltemperature",
    "Models.Climate.Weather": "weather",
    "Models.Clock": "clock",
    "Models.MicroClimate": "microclimate",
    "Models.Irrigation": "irrigation",
    "Models.Soils.IrrigationApplicationType": "irrigation",
    "Models.Fertiliser": "fertiliser",
    "Models.FertiliserApplicationType": "fertiliser",
    "Models.FertiliserType": "fertilisertype",
    "Models.Surface.SurfaceOrganicMatter": "surfaceorganicmatter",
    "Models.Surface.ResidueType": "residuetype",
    "Models.Soils.TillageType": "tillage",
    "Models.WaterModel.CNReductionForTillage": "tillage",
    "Models.PMF.SowingParameters": "sowing",
    "Models.PMF.Plant": "plant",
    "Models.PMF.Biomass": "biomass",
    "Models.PMF.CompositeBiomass": "biomass",
    "Models.PMF.Organ": "organ",
    "Models.PMF.Organs.GenericOrgan": "organ",
    "Models.PMF.Organs.Leaf": "leaf",
    "Models.PMF.Organs.SimpleLeaf": "leaf",
    "Models.PMF.Organs.PerennialLeaf": "leaf",
    "Models.PMF.Organs.Root": "root",
    "Models.PMF.RootNetwork": "root",
    "Models.PMF.Organs.Nodule": "nodule",
    "Models.PMF.Organs.ReproductiveOrgan": "reproductiveorgan",
    "Models.PMF.Organs.HIReproductiveOrgan": "reproductiveorgan",
    "Models.PMF.Organs.LeafCohort": "leafcohort",
    "Models.PMF.Organs.LeafCohortParameters": "leafcohort",
    "Models.PMF.Organs.PerennialLeafCohort": "leafcohort",
    "Models.AgPasture.PastureSpecies": "plant",
    "Models.AgPasture.PastureSpeciesConstants": "plant",
    "Models.AgPasture.PastureAboveGroundOrgan": "shoot",
    "Models.AgPasture.PastureBelowGroundOrgan": "root",
    "Models.Soils.Nutrients.Nutrient": "nutrient",
    "Models.Soils.Nutrients.OrganicPool": "organicpool",
    "Models.Soils.Nutrients.NH3Volatilisation": "volatilisation",
    "Models.Functions.PhotoperiodFunction": "photoperiod",
    "Models.Functions.SupplyFunctions.RUEModel": "photosynthesis",
    "Models.Functions.SupplyFunctions.RUECO2Function": "photosynthesis",
    "Models.Functions.SupplyFunctions.LeafLightUseEfficiency": "photosynthesis",
    "Models.Functions.SupplyFunctions.LeafMaxGrossPhotosynthesis": "photosynthesis",
    "Models.Functions.SupplyFunctions.CanopyPhotosynthesis": "photosynthesis",
    "Models.Functions.SupplyFunctions.CanopyGrossPhotosynthesisHourly": "photosynthesis",
    "Models.Functions.SupplyFunctions.StomatalConductanceCO2Modifier": "photosynthesis",
    "Models.Functions.SupplyFunctions.LimitedTranspirationRate": "transpiration",
}
STRUCTURAL_KEYS = {
    "$type", "Children", "Name", "ResourceName", "Enabled", "ReadOnly",
    "Version", "IsHidden", "IncludeInDocumentation", "ShowInDocs", "Structure",
    "IsInitialising", "IncludeInSummary", "OnCreated", "Parent",
    "ReadDocumentationComments", "ParentType", "FullPath", "MemoText",
    "FractionComplete", "VegetativePhaseCalculationWheat", "ShowYears", "StartYear",
}


MANAGEMENT_TOOLBOX = 'ApsimNG/Resources/Toolboxes/ManagementToolbox.apsimx'
# Existing official rules, categorised by practice, not by crop or C# class.
MANAGEMENT_RULES = {
    'Sow on a fixed date': 'sowing',
    'Sow using a variable rule': 'autosowing',
    'Harvesting': 'harvest',
    'Tillage on a fixed date': 'tillage',
    'Set Emergence Date': 'emergence',
    'Fertilise on fixed dates': 'fertiliser',
    'Fertilise on fixed dates (advanced version)': 'autofertiliser',
    'Fertilise at sowing': 'fertiliser',
    'Fertilise on Zadok stage': 'autofertiliser',
    'Fertilise topup': 'autofertiliser',
    'Fertilise when variable reaches threshold': 'autofertiliser',
    'Fertilise on event': 'autofertiliser',
    'AddSlurryToSOM': 'organicfertiliser',
    'IncorporateSlurryIntoSoil': 'organicfertiliser',
    'AddManure on a fixed date': 'organicfertiliser',
    'Irrigate on fixed dates': 'irrigation',
    'AutomaticIrrigation': 'autoirrigation',
    'Automatic irrigation based on water deficit': 'autoirrigation',
    'Irrigate to crop PAW at sowing': 'irrigation',
    'CentrePivot': 'autoirrigation',
    'Furrow Irrigation': 'autoirrigation',
    'Reset on sowing': 'reset',
    'Reset on date': 'reset',
}

# Reviewed against runtime assignments, not inferred from a public setter.
VERIFIED_ROLES = {
    ('Models.PMF.Struct.Culm', 'FinalLeafNo'): {
        'status': 'output',
        'reason': 'Final leaf number is assigned to culms during potential leaf-area calculation',
        'evidence': 'Models/PMF/Structure/LeafCulms.cs#L213',
    },
    ('Models.Soils.Nutrients.NH3Volatilisation', 'pH'): {
        'status': 'output',
        'reason': 'Declared under Outputs; initialised from Chemical.PH and updated during simulation',
        'evidence': 'Models/Soils/Nutrients/NH3Volatilisation.cs#L199',
    },
}

# Clarifications verified against the process code; keep the original metadata
# in sources rather than replacing it with an unattributed interpretation.
VERIFIED_METADATA = {
    ('tillage', 'cn_rain'): {
        'Description': (
            'Cumulative rainfall threshold after tillage at which the tillage-induced '
            'reduction in runoff Curve Number (CN) disappears. The initial reduction '
            'cn_red decreases linearly to zero as this threshold is reached. '
            'WaterBalance accumulates PotentialRunoff, including irrigation flagged '
            'WillRunoff, towards this threshold.'
        ),
        'unit': 'mm',
        'evidence': [
            'Models/Soils/WaterModel/WaterBalance.cs#L863-L877',
            'Models/Soils/WaterModel/Runoff.cs#L68-L75',
            'Models/Soils/WaterModel/Runoff.cs#L133',
            'Models/Soils/WaterModel/CNReductionForTillage.cs#L56-L69',
            'Models/Soils/WaterModel/WaterBalance.cs#L344-L357',
        ],
    },
}


def category_for_class(name):
    if name in CATEGORIES:
        return CATEGORIES[name]
    if name.startswith("Models.PMF.Phen.") and name.count(".") == 3:
        return "phenology"
    if name.startswith("Models.PMF.Struct.") and name.count(".") == 3:
        return "structure"
    return None


def value_type(type_name):
    """Data values only, never model objects, parsers or event records."""
    scalar = type_name.strip().rstrip("?")
    scalars = {"double", "float", "int", "long", "short", "decimal", "bool",
               "string", "String", "DateTime", "uint", "byte"}
    if scalar in scalars or scalar.endswith("Enum"):
        return True
    if scalar.endswith("[]"):
        return value_type(scalar[:-2])
    match = re.fullmatch(r"(?:IReadOnlyList|IEnumerable|List)<(.+)>", scalar)
    return bool(match and value_type(match[1]))


def eligible(info):
    return (info["name"] not in STRUCTURAL_KEYS
            and not info["name"].endswith(("Metadata", "()"))
            and info["role"] != "link"
            and value_type(info["type"]))


def git(root, *args):
    return subprocess.check_output(["git", *args], cwd=root, text=True).strip()


def text(node, source):
    return source[node.start_byte:node.end_byte].decode("utf-8") if node else ""


def walk(node):
    yield node
    for child in node.named_children:
        yield from walk(child)


def string_value(raw):
    if raw.startswith('@"') and raw.endswith('"'):
        return raw[2:-1].replace('""', '"')
    if raw.startswith('"') and raw.endswith('"'):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None
    return None


def literal(node, source):
    """Only explicit literals; no implicit CLR defaults or expression evaluation."""
    if node is None:
        return None, False
    raw = text(node, source).strip()
    if node.type in {"string_literal", "verbatim_string_literal"}:
        value = string_value(raw)
        return value, value is not None
    if raw in {"true", "false", "null"}:
        return {"true": True, "false": False, "null": None}[raw], True
    if re.fullmatch(r"[-+]?\d[\d_]*(?:\.\d[\d_]*)?(?:[eE][-+]?\d+)?[fFdDmMuUlL]*", raw):
        clean = re.sub(r"[fFdDmMuUlL]+$", "", raw).replace("_", "")
        return float(clean) if any(c in clean for c in ".eE") else int(clean), True
    if node.type in {"array_creation_expression", "implicit_array_creation_expression", "initializer_expression"}:
        initializer = node if node.type == "initializer_expression" else next(
            (c for c in node.named_children if c.type == "initializer_expression"), None)
        if initializer is not None:
            values = [literal(c, source) for c in initializer.named_children]
            if all(known for _, known in values):
                return [value for value, _ in values], True
    return None, False


def serialise(value):
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def attributes(node, source):
    result = {}
    for child in node.named_children:
        if child.type != "attribute_list":
            continue
        for attr in child.named_children:
            if attr.type != "attribute":
                continue
            name = text(attr.child_by_field_name("name"), source).split(".")[-1]
            args = next((n for n in attr.named_children if n.type == "attribute_argument_list"), None)
            values = {}
            if args:
                for index, arg in enumerate(args.named_children):
                    key = text(arg.child_by_field_name("name"), source) or str(index)
                    value = arg.named_children[-1]
                    parsed, known = literal(value, source)
                    values[key] = parsed if known else text(value, source)
            result[name.removesuffix("Attribute")] = values
    return result


def doc_summary(node, source):
    previous = node.prev_named_sibling
    comments = []
    while previous is not None and previous.type == "comment":
        raw = text(previous, source)
        if not raw.startswith("///"):
            break
        comments.insert(0, re.sub(r"(?m)^\s*///\s?", "", raw))
        previous = previous.prev_named_sibling
    raw = "\n".join(comments)
    match = re.search(r"<summary>(.*?)</summary>", raw, re.S)
    if not match:
        return ""
    summary = re.sub(r'<(?:see|paramref|typeparamref)\s+(?:cref|name)="([^"]+)"\s*/>', r"\1", match[1])
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", summary)).split())


def class_name(node, source):
    names = []
    parent = node
    namespace = ""
    while parent:
        if parent.type in {"class_declaration", "struct_declaration", "record_declaration"}:
            names.insert(0, text(parent.child_by_field_name("name"), source))
        if parent.type in {"namespace_declaration", "file_scoped_namespace_declaration"}:
            namespace = text(parent.child_by_field_name("name"), source)
        parent = parent.parent
    if not namespace:
        root = node
        while root.parent:
            root = root.parent
        ns = next((c for c in root.named_children if c.type == "file_scoped_namespace_declaration"), None)
        namespace = text(ns.child_by_field_name("name"), source) if ns else ""
    return ".".join(filter(None, [namespace, *names]))


def member_specs(node, source):
    """Public readable data members plus zero-required-argument functions."""
    modifiers = {text(c, source) for c in node.named_children if c.type == "modifier"}
    if "public" not in modifiers:
        return []
    type_name = text(node.child_by_field_name("type"), source)
    if node.type == "property_declaration":
        accessors = node.child_by_field_name("accessors")
        getter, setter = False, False
        if accessors:
            for accessor in accessors.named_children:
                if accessor.type != "accessor_declaration":
                    continue
                restricted = any(text(c, source) in {"private", "protected", "internal"}
                                 for c in accessor.named_children if c.type == "modifier")
                keyword = next((c.type for c in accessor.children if c.type in {"get", "set", "init"}), "")
                getter |= keyword == "get" and not restricted
                setter |= keyword in {"set", "init"} and not restricted
        elif any(c.type == "arrow_expression_clause" for c in node.named_children):
            getter = True
        return [(text(node.child_by_field_name("name"), source), type_name,
                 node.child_by_field_name("value"), getter, setter)]
    if node.type == "field_declaration":
        declaration = next(c for c in node.named_children if c.type == "variable_declaration")
        type_name = text(declaration.child_by_field_name("type"), source)
        return [(text(c.child_by_field_name("name"), source), type_name,
                 c.named_children[-1] if any(t.type == "=" for t in c.children) else None,
                 True, not bool(modifiers & {"readonly", "const"}))
                for c in declaration.named_children if c.type == "variable_declarator"]
    if node.type == "method_declaration":
        type_name = text(node.child_by_field_name("returns"), source)
        parameters = node.child_by_field_name("parameters")
        required = [c for c in parameters.named_children if c.type == "parameter"
                    and not any(t.type == "=" for t in c.children)] if parameters else []
        if type_name not in {"void", "Task", ""} and not required and "static" not in modifiers:
            return [(text(node.child_by_field_name("name"), source) + "()", type_name, None, True, False)]
    return []


def extract_sources(root, revision):
    parser = Parser(Language(tree_sitter_c_sharp.language()))
    members, classes, issues = {}, {}, []
    files = sorted((root / "Models").rglob("*.cs"))
    for path in files:
        try:
            decoded = path.read_text(encoding='utf-8-sig')
        except UnicodeDecodeError:
            decoded = path.read_text(encoding='cp1252')
        source = decoded.encode('utf-8')
        tree = parser.parse(source)
        if tree.root_node.has_error:
            issues.append(str(path.relative_to(root)))
        for node in walk(tree.root_node):
            if node.type not in {"class_declaration", "struct_declaration", "record_declaration"}:
                continue
            full_name = class_name(node, source)
            bases = next((c for c in node.named_children if c.type == "base_list"), None)
            classes.setdefault(full_name, {"summary": doc_summary(node, source),
                                          "bases": text(bases, source), "members": {}})
            body = node.child_by_field_name("body")
            if body is None:
                continue
            for declaration in body.named_children:
                if declaration.type not in {"property_declaration", "field_declaration", "method_declaration"}:
                    continue
                attrs = attributes(declaration, source)
                description = attrs.get("Description", {}).get("0") or doc_summary(declaration, source)
                specs = member_specs(declaration, source)
                private_links = not specs and 'Link' in attrs and declaration.type == 'field_declaration'
                if private_links:
                    variable = next(c for c in declaration.named_children if c.type == 'variable_declaration')
                    link_type = text(variable.child_by_field_name('type'), source)
                    specs = [(text(c.child_by_field_name('name'), source), link_type, None, True, False)
                             for c in variable.named_children if c.type == 'variable_declarator']
                for name, type_name, initializer, getter, setter in specs:
                    if not getter and not setter:
                        continue
                    # Framework structure is not a scientific variable or a serialised input.
                    if name == "Structure":
                        continue
                    default, known = literal(initializer, source)
                    if not known and "DefaultValue" in attrs:
                        for attr in (c for c in walk(declaration) if c.type == "attribute"):
                            attr_name = text(attr.child_by_field_name("name"), source)
                            if attr_name.split(".")[-1].removesuffix("Attribute") != "DefaultValue":
                                continue
                            arg = next((c for c in walk(attr) if c.type == "attribute_argument"), None)
                            if arg:
                                default, known = literal(arg.named_children[-1], source)
                    if name.endswith("()"):
                        role = "output_method"
                    elif "Link" in attrs:
                        role = "link"
                    elif ({"JsonIgnore", "NonSerialized"} & attrs.keys()) and getter:
                        role = "output"
                    elif getter and setter:
                        role = "input_output"
                    elif setter:
                        role = "input"
                    else:
                        role = "output"
                    rel = path.relative_to(root).as_posix()
                    line = declaration.start_point.row + 1
                    info = {
                        "class": full_name, "name": name, "type": type_name,
                        "private_link": private_links,
                        "member_kind": declaration.type,
                        "proxy_parameter": bool(re.search(r"return\s+constants\.", text(declaration, source))),
                        "computed_getter": (declaration.type == 'property_declaration' and
                            any(c.type in {'block', 'arrow_expression_clause'}
                                for accessor in declaration.named_children if accessor.type == 'accessor_list'
                                for get in accessor.named_children
                                if get.type == 'accessor_declaration' and any(k.type == 'get' for k in get.children)
                                for c in get.named_children)),
                        "description": description, "unit": attrs.get("Units", {}).get("0", ""),
                        "minimum": attrs.get("Bounds", {}).get("Lower", ""),
                        "maximum": attrs.get("Bounds", {}).get("Upper", ""),
                        "default": default, "known_default": known,
                        "default_kind": "initializer" if initializer else "DefaultValue_attribute",
                        "initializer": text(initializer, source), "role": role,
                        "attributes": sorted(attrs), "path": rel, "line": line,
                        "url": f"https://github.com/APSIMInitiative/ApsimX/blob/{revision}/{quote(rel)}#L{line}",
                    }
                    key = (full_name, name)
                    if key in members:
                        # Partial declarations may expose the same property once per build branch.
                        info["duplicate_declaration"] = members[key]["url"]
                    members[key] = info
                    classes[full_name]["members"][name] = info
    return members, classes, files, issues


def inherited_member(classes, full_name, name, seen=None):
    seen = set() if seen is None else seen
    if full_name in seen or full_name not in classes:
        return None
    seen.add(full_name)
    cls = classes[full_name]
    if name in cls["members"]:
        return cls["members"][name]
    namespace = full_name.rsplit(".", 1)[0]
    for base in re.findall(r"[\w.]+", cls["bases"]):
        candidates = [base, namespace + "." + base, "Models.Core." + base]
        if not any(candidate in classes for candidate in candidates):
            matches = [key for key in classes if key.endswith("." + base)]
            if len(matches) == 1:
                candidates.extend(matches)
        for candidate in candidates:
            found = inherited_member(classes, candidate, name, seen)
            if found:
                return found
    return None


def resource_parameters(root, revision, classes):
    """Direct documented scalar parameters, shared across crop instances.

    A Constant child is included only when its biological parent declares the
    matching IFunction dependency. Nested function wiring and crop values are
    deliberately not exported as independent agronomic parameters.
    """
    found, files = [], []

    def visit(node):
        if not isinstance(node, dict):
            return
        full_name = str(node.get('$type', '')).split(',')[0]
        if full_name in {'Models.PMF.Cultivar', 'Models.Manager', 'Models.Memo'}:
            return
        category = category_for_class(full_name)
        if category:
            for child in node.get('Children', []):
                if str(child.get('$type', '')).split(',')[0] != 'Models.Functions.Constant':
                    continue
                name = child.get('Name', '')
                link = inherited_member(classes, full_name, name)
                if link is None:
                    # Link(ByName=true) fields use both lower and upper initials.
                    parent = classes.get(full_name, {})
                    matches = [v for k, v in parent.get('members', {}).items()
                               if k.casefold() == name.casefold() and v['role'] == 'link']
                    link = matches[0] if len(matches) == 1 else None
                if not link or link['role'] != 'link' or 'IFunction' not in link['type']:
                    continue
                if not link.get('description'):
                    continue
                definition = inherited_member(classes, 'Models.Functions.Constant', 'FixedValue')
                parameter = dict(definition, description=link['description'],
                                 unit=link.get('unit', ''), resource_parameter=True,
                                 known_default=False, resource_path=name,
                                 biological_definition=link['url'])
                if category == 'phenology' and full_name != 'Models.PMF.Phen.Phenology':
                    name = str(node.get('Name', 'Phase')) + '.' + name
                found.append((category, name, parameter))
        for child in node.get('Children', []):
            visit(child)

    for path in sorted((root / 'Models' / 'Resources').rglob('*.json')):
        files.append(path.relative_to(root).as_posix())
        visit(json.loads(path.read_text(encoding='utf-8-sig')))
    return found, files


def management_variables(root, revision):
    """Extract public input properties and state variables, never methods.

    Parameter names come from each official rule's Parameters collection.
    Toolbox example values are provenance only, not model-wide defaults.
    """
    path = root / MANAGEMENT_TOOLBOX
    if not path.exists():
        raise ValueError(f'Missing official management toolbox: {path}')
    document = json.loads(path.read_text(encoding='utf-8-sig'))
    parser = Parser(Language(tree_sitter_c_sharp.language()))
    variables, rules, issues = [], [], []
    url = f'https://github.com/APSIMInitiative/ApsimX/blob/{revision}/{MANAGEMENT_TOOLBOX}'

    def visit(node):
        rule = node.get('Name', '')
        if node.get('$type', '').split(',')[0] == 'Models.Manager' and rule in MANAGEMENT_RULES:
            category = MANAGEMENT_RULES[rule]
            code = '\n'.join(node.get('CodeArray', [])) or node.get('Code', '')
            source = code.encode('utf-8')
            tree = parser.parse(source)
            if tree.root_node.has_error:
                issues.append({'rule': rule, 'error': 'Unrecognised C# syntax'})
            parameters = {p['Key']: p.get('Value') for p in node.get('Parameters', [])}
            seen = set()
            for declaration in walk(tree.root_node):
                if declaration.type not in {'property_declaration', 'field_declaration'}:
                    continue
                attrs = attributes(declaration, source)
                for name, type_name, initializer, getter, setter in member_specs(declaration, source):
                    if 'Link' in attrs or name in STRUCTURAL_KEYS or name.endswith('Metadata'):
                        continue
                    is_input = name in parameters
                    if not is_input and not value_type(type_name):
                        continue
                    if is_input:
                        seen.add(name)
                    elif not getter:
                        continue
                    description = attrs.get('Description', {}).get('0') or doc_summary(declaration, source)
                    unit = attrs.get('Units', {}).get('0', '')
                    if not unit and description:
                        # Read units explicitly written in the rule's Description.
                        candidates = re.findall(r'\(([^()]+)\)', description)
                        unit = next((v for v in candidates if v in {
                            'mm', 'kg/ha', 'd', 'days', '/m2', '0-1', '%', 'degrees',
                            'dd-mmm', 'd-mmm', 't wet weight/ha', 'kg DM /kg wet slurry'}), '')
                    default, known = literal(initializer, source)
                    info = {
                        'class': 'Models.Script', 'name': name, 'type': type_name,
                        'description': description, 'unit': unit,
                        'minimum': attrs.get('Bounds', {}).get('Lower', ''),
                        'maximum': attrs.get('Bounds', {}).get('Upper', ''),
                        'role': 'input' if is_input else 'output',
                        'known_default': known, 'default': default,
                        'attributes': sorted(attrs), 'path': MANAGEMENT_TOOLBOX,
                        'line': declaration.start_point.row + 1, 'url': url,
                        'manager_rule': rule, 'manager_category': category,
                        'qualifier': re.sub(r'[^A-Za-z0-9]+', '', rule),
                    }
                    # No cultivar names or model references become generic values.
                    if is_input and value_type(type_name) and 'cultivar' not in name.casefold():
                        info['toolbox_example'] = parameters[name]
                    variables.append(info)
            missing = sorted(parameters.keys() - seen)
            if missing:
                issues.append({'rule': rule, 'parameters_without_declaration': missing})
            rules.append({'rule': rule, 'category': category, 'parameters': sorted(seen)})
        for child in node.get('Children', []):
            visit(child)

    visit(document)
    absent = MANAGEMENT_RULES.keys() - {r['rule'] for r in rules}
    if absent:
        raise ValueError(f'Missing official management rules: {sorted(absent)}')
    return variables, rules, issues


def build_rows(columns, members, resource_members, revision, extracted_on, management_members=()):
    groups = {}
    excluded = Counter()
    for info in members.values():
        category = category_for_class(info['class'])
        if not category or not eligible(info):
            excluded['technical_or_outside_crop_dictionary'] += 1
            continue
        if info.get('proxy_parameter'):
            continue
        if category == 'soil' and info['name'] not in {'Latitude', 'Longitude', 'SoilType'}:
            continue
        runtime_field = (info.get('member_kind') == 'field_declaration'
                         and not {'Description', 'Display', 'Bounds'} & set(info['attributes'])
                         and category not in {'irrigation', 'fertiliser', 'sowing'})
        computed_state = (info.get('computed_getter')
                          and not {'Description', 'Display', 'Bounds'} & set(info['attributes']))
        status = 'output' if (info['role'] == 'output' or category == 'biomass' or runtime_field or computed_state
                              or (category == 'photoperiod' and info['name'] == 'DayLength')) else 'input'
        reviewed = VERIFIED_ROLES.get((info['class'], info['name']))
        if reviewed:
            status = reviewed['status']
            info = dict(info, classification_review={
                **reviewed,
                'evidence': f'https://github.com/APSIMInitiative/ApsimX/blob/{revision}/{reviewed["evidence"]}',
            })
        table = 'outputs' if status == 'output' else category
        # Organ/system context disambiguates e.g. Root.Depth and Sowing.Depth.
        field = category + '.' + info['name'] if status == 'output' else info['name']
        groups.setdefault((table, field), []).append(info)
    for category, field, info in resource_members:
        groups.setdefault((category, field), []).append(info)
    for info in management_members:
        category = info['manager_category']
        table = 'outputs' if info['role'] == 'output' else category
        field = category + '.' + info['name'] if table == 'outputs' else info['name']
        groups.setdefault((table, field), []).append(info)

    # ParameterResolver normalises names with casefold; merge casing variants
    # such as LAIDead/LaiDead before checking their type and unit compatibility.
    canonical_groups = {}
    for (table, field), variants in groups.items():
        key = table, field.casefold()
        if key not in canonical_groups:
            canonical_groups[key] = (field, [])
        canonical_groups[key][1].extend(variants)
    groups = {(table, field): variants
              for (table, _), (field, variants) in canonical_groups.items()}

    # A homonym with different dimensions or types is not a shared variable.
    # Keep the biological implementation context, never a crop name.
    def canonical_unit(unit):
        if unit in {'-', '0-1'}:
            return '-'
        return unit.replace('^', '').replace(' ', '')

    separated, disambiguated = {}, []
    for (table, field), variants in groups.items():
        types = {v['type'] for v in variants}
        units = {canonical_unit(v.get('unit', '')) for v in variants if v.get('unit')}
        if len(types) > 1 or len(units) > 1:
            for v in variants:
                qualifier = v.get('qualifier') or v['class'].split('.')[-1]
                qualified = (field.rsplit('.', 1)[0] + '.' + qualifier + '.' + field.rsplit('.', 1)[1]
                             if table == 'outputs' else qualifier + '.' + field)
                separated.setdefault((table, qualified), []).append(v)
            disambiguated.append({'Table': table, 'Champ': field,
                                  'reason': 'different types or units; retained implementation context'})
        else:
            separated[table, field] = variants

    rows, conflicts = [], []
    for (table, field), variants in sorted(separated.items()):
        # Same class/member across resources is a single definition.
        unique = {(v['class'], v['name'], v.get('biological_definition', ''), v.get('manager_rule', '')): v for v in variants}
        variants = list(unique.values())
        base = sorted(variants, key=lambda v: (bool(v.get('resource_parameter')), v['class']))[0]
        row = dict.fromkeys(columns, '')
        category = field.split('.')[0] if table == 'outputs' else table
        row.update(model='apsim', Table=table, Champ=field, domain=category,
                   status='output' if table == 'outputs' else 'input',
                   SuiviModif=f'APSIM NG {revision}; functional catalogue {extracted_on}')
        details = {'category': category, 'sources': [
            {'class': v['class'], 'member': v['name'], 'definition': v['url'],
             'description': v.get('description', ''), 'unit': v.get('unit', ''),
             'type': v['type'], 'minimum': v.get('minimum', ''),
             'maximum': v.get('maximum', ''),
             **({'classification_review': v['classification_review']}
                if v.get('classification_review') else {}),
             **({'management_rule': v['manager_rule'], 'script_line': v['line'],
                 **({'toolbox_example': v['toolbox_example']} if 'toolbox_example' in v else {})}
                if v.get('manager_rule') else {}),
             **({'parameter_path': v['resource_path'], 'biological_definition': v['biological_definition']} if v.get('resource_parameter') else {})}
            for v in variants]}
        row['Description'] = base.get('description', '')
        for key, col in [('type', 'Type'), ('unit', 'unit'), ('minimum', 'Minnval'), ('maximum', 'Maxval')]:
            values = {(canonical_unit(str(v[key])) if key == 'unit' else str(v[key]))
                      for v in variants if v.get(key, '') != ''}
            if len(values) == 1:
                row[col] = (next(v['unit'] for v in variants if v.get('unit')).replace('^', '')
                            if key == 'unit' else values.pop())
            elif len(values) > 1:
                details.setdefault('metadata_conflicts', {})[col] = sorted(values)
        code_variants = [v for v in variants if not v.get('resource_parameter')]
        # A universal default needs explicit agreement across every declaration.
        if (table != 'outputs' and code_variants and len(code_variants) == len(variants)
                and all(v['known_default'] and v['default'] is not None for v in variants)
                and len({serialise(v['default']) for v in variants}) == 1):
            row['defaultvalueYN'] = 1
            row['defaultValueOtherSource'] = serialise(base['default'])
            row['SourceDV'] = base['url']
        else:
            row['defaultvalueYN'] = 0
        if any(v.get('resource_parameter') for v in variants):
            details['default_policy'] = 'No culture-specific resource value used as a generic default'
        if any(v.get('manager_rule') for v in variants):
            details['management_scope'] = 'Official management rules; sample settings are not model-wide defaults'
        if 'metadata_conflicts' in details:
            conflicts.append({'Table': table, 'Champ': field, 'conflicts': details['metadata_conflicts']})
        reviewed_metadata = VERIFIED_METADATA.get((table, field))
        if reviewed_metadata:
            row.update({k: v for k, v in reviewed_metadata.items() if k != 'evidence'})
            details['metadata_review'] = {
                **reviewed_metadata,
                'evidence': [f'https://github.com/APSIMInitiative/ApsimX/blob/{revision}/{path}'
                             for path in reviewed_metadata['evidence']],
            }
        row['commentaire'] = json.dumps(details, ensure_ascii=False, separators=(',', ':'))
        rows.append(row)
    return rows, conflicts, excluded, disambiguated


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--apsim-source', required=True, type=Path)
    cli.add_argument('--dictionary-schema', required=True, type=Path)
    cli.add_argument('--output-dir', required=True, type=Path)
    cli.add_argument('--extracted-on', required=True, type=date.fromisoformat)
    args = cli.parse_args()
    root = args.apsim_source.resolve()
    revision = git(root, 'rev-parse', 'HEAD')
    if git(root, 'status', '--porcelain', '--', 'Models', MANAGEMENT_TOOLBOX):
        raise ValueError('The upstream Models checkout must be unmodified')
    with sqlite3.connect(args.dictionary_schema.resolve().as_uri() + '?mode=ro', uri=True) as conn:
        columns = [r[1] for r in conn.execute('PRAGMA table_info(Variables)')]
    if not columns:
        raise ValueError('Variables table is missing')
    members, classes, cs_files, parse_issues = extract_sources(root, revision)
    parameters, resources = resource_parameters(root, revision, classes)
    management, management_rules, management_issues = management_variables(root, revision)
    rows, conflicts, excluded, disambiguated = build_rows(
        columns, members, parameters, revision, args.extracted_on.isoformat(), management)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = args.output_dir / 'Variables_APSIMNG.csv'
    with output.open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    manifest = {
        'source': 'https://github.com/APSIMInitiative/ApsimX', 'revision': revision,
        'revision_date': git(root, 'show', '-s', '--format=%cI', 'HEAD'),
        'documentation': 'https://docs.apsim.info/docs/',
        'extracted_on': args.extracted_on.isoformat(),
        'schema_columns': columns, 'rows': len(rows),
        'table_counts': dict(Counter(r['Table'] for r in rows)),
        'status_counts': dict(Counter(r['status'] for r in rows)),
        'with_description': sum(bool(r['Description']) for r in rows),
        'with_explicit_default': sum(r['defaultvalueYN'] == 1 for r in rows),
        'excluded': dict(excluded), 'csharp_files_read': len(cs_files),
        'resource_files_read': len(resources),
        'management_toolbox': MANAGEMENT_TOOLBOX, 'management_rules': management_rules,
        'management_variables': len(management), 'management_issues': management_issues,
        'metadata_conflicts': conflicts, 'disambiguated_homonyms': disambiguated,
        'parser_issues_outside_scope': parse_issues,
        'parser_issues_in_catalogue': sorted({v['path'] for v in members.values()
             if category_for_class(v['class']) and eligible(v)} & set(parse_issues)),
        'duplicate_keys': [list(key) for key, count in Counter((r['Table'],r['Champ']) for r in rows).items() if count > 1],
        'csv_sha256': hashlib.sha256(output.read_bytes()).hexdigest(),
    }
    (args.output_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
    print(json.dumps({k: v for k, v in manifest.items() if k not in {'schema_columns','metadata_conflicts','management_rules'}}, indent=2))


if __name__ == '__main__':
    main()
