#!/usr/bin/env python3
"""Convert STICS V9 plant/cultivar files to the layout of a V11 template."""

from __future__ import annotations

import argparse
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


Pair = tuple[str, str]


@dataclass(frozen=True)
class ConversionResult:
    """Description of the parameter mapping performed by one conversion."""

    varieties: int
    keep_specie_param_v9_to_v11: tuple[str, ...]
    move_specie_param_v9_to_v11: tuple[str, ...]
    new_specie_param_v11: tuple[str, ...]
    drop_specie_param_v9: tuple[str, ...]
    keep_variety_param_v9_to_v11: tuple[str, ...]
    new_variety_param_v11: tuple[str, ...]
    drop_variety_param_v9: tuple[str, ...]

    @property
    def species_from_v9(self) -> int:
        return len(self.keep_specie_param_v9_to_v11)

    @property
    def species_from_default(self) -> int:
        return len(self.new_specie_param_v11)

    @property
    def variety_from_v9(self) -> int:
        return len(self.keep_variety_param_v9_to_v11)

    @property
    def moved_from_species(self) -> int:
        return len(self.move_specie_param_v9_to_v11)

    @property
    def variety_from_default(self) -> int:
        return len(self.new_variety_param_v11)


def read_pairs(path: Path) -> list[Pair]:
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    if len(lines) % 2:
        raise ValueError(f"{path}: nombre impair de lignes ({len(lines)})")
    return [(lines[index], lines[index + 1]) for index in range(0, len(lines), 2)]


def split_sections(pairs: list[Pair], path: Path) -> tuple[list[Pair], list[list[Pair]]]:
    starts = [
        index for index, (parameter, _) in enumerate(pairs)
        if parameter.strip().casefold() == "codevar"
    ]
    if not starts:
        raise ValueError(f"{path}: aucun paramètre codevar")

    species = pairs[: starts[0]]
    varieties = [
        pairs[start : starts[position + 1] if position + 1 < len(starts) else len(pairs)]
        for position, start in enumerate(starts)
    ]
    return species, varieties


def as_values(pairs: Iterable[Pair]) -> dict[str, str]:
    values: dict[str, str] = {}
    for parameter, value in pairs:
        if parameter in values:
            raise ValueError(f"paramètre dupliqué dans une section: {parameter}")
        values[parameter] = value
    return values


def named_varieties(blocks: Iterable[list[Pair]]) -> OrderedDict[str, list[Pair]]:
    result: OrderedDict[str, list[Pair]] = OrderedDict()
    for block in blocks:
        if not block or block[0][0].strip().casefold() != "codevar":
            raise ValueError("un bloc variété ne commence pas par codevar")
        name = block[0][1]
        if name in result:
            raise ValueError(f"codevar dupliqué: {name}")
        result[name] = block
    return result


def convert(source: Path, template: Path, destination: Path) -> ConversionResult:
    source_species_pairs, source_variety_blocks = split_sections(read_pairs(source), source)
    template_species_pairs, template_variety_blocks = split_sections(read_pairs(template), template)

    source_species = as_values(source_species_pairs)
    source_varieties = named_varieties(source_variety_blocks)
    template_varieties = named_varieties(template_variety_blocks)
    first_template = next(iter(template_varieties.values()))

    output: list[Pair] = []
    for parameter, default_value in template_species_pairs:
        if parameter in source_species:
            output.append((parameter, source_species[parameter]))
        else:
            output.append((parameter, default_value))

    for variety_name, source_block in source_varieties.items():
        source_values = as_values(source_block[1:])
        template_block = template_varieties.get(variety_name, first_template)
        output.append((template_block[0][0], variety_name))

        for parameter, default_value in template_block[1:]:
            if parameter in source_values:
                value = source_values[parameter]
            elif parameter in source_species:
                value = source_species[parameter]
            else:
                value = default_value
            output.append((parameter, value))

    destination.parent.mkdir(parents=True, exist_ok=True)
    text = "".join(f"{parameter}\r\n{value}\r\n" for parameter, value in output)
    destination.write_bytes(text.encode("utf-8"))
    template_species_names = {parameter for parameter, _ in template_species_pairs}
    template_variety_names = {
        parameter for block in template_varieties.values() for parameter, _ in block[1:]
    }
    source_variety_names = {
        parameter for block in source_varieties.values() for parameter, _ in block[1:]
    }

    return ConversionResult(
        varieties=len(source_varieties),
        keep_specie_param_v9_to_v11=tuple(sorted(source_species.keys() & template_species_names)),
        move_specie_param_v9_to_v11=tuple(
            sorted((source_species.keys() - template_species_names) & template_variety_names)
        ),
        new_specie_param_v11=tuple(sorted(template_species_names - source_species.keys())),
        drop_specie_param_v9=tuple(
            sorted(source_species.keys() - template_species_names - template_variety_names)
        ),
        keep_variety_param_v9_to_v11=tuple(
            sorted(source_variety_names & template_variety_names)
        ),
        new_variety_param_v11=tuple(
            sorted(template_variety_names - source_variety_names - source_species.keys())
        ),
        drop_variety_param_v9=tuple(sorted(source_variety_names - template_variety_names)),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--template", required=True, type=Path, help="fichier plante V11 modèle")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("sources", nargs="+", type=Path, help="fichiers plante/cultivars V9")
    args = parser.parse_args()

    for source in args.sources:
        destination = args.output_dir / source.name
        stats = convert(source, args.template, destination)
        print(
            f"{source.name}: {stats.varieties} variété(s), "
            f"espèce V9={stats.species_from_v9}, défaut V11={stats.species_from_default}, "
            f"variété V9={stats.variety_from_v9}, "
            f"espèce V9 déplacée={stats.moved_from_species}, "
            f"défaut V11={stats.variety_from_default}"
        )


if __name__ == "__main__":
    main()
