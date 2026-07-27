"""Read, inspect and edit STICS plant/cultivar parameter files."""

from __future__ import annotations

from collections import OrderedDict
from pathlib import Path
from typing import Mapping, Optional, Union


PathLike = Union[str, Path]
Parameters = OrderedDict[str, str]


class SticsCultivarFile:
    """Mutable representation of a STICS plant/cultivar text file.

    STICS files contain alternating parameter/value lines. Parameters before
    the first ``codevar`` belong to the species; every ``codevar`` starts a
    cultivar block.
    """

    def __init__(
        self,
        species_parameters: Mapping[str, str],
        cultivars: Mapping[str, Mapping[str, str]],
        path: Optional[PathLike] = None,
    ) -> None:
        self.path = Path(path) if path is not None else None
        self.species_parameters: Parameters = OrderedDict(
            (name, str(value)) for name, value in species_parameters.items()
        )
        self.cultivars: OrderedDict[str, Parameters] = OrderedDict(
            (name, OrderedDict((parameter, str(value)) for parameter, value in values.items()))
            for name, values in cultivars.items()
        )
        self._validate()

    @classmethod
    def read(cls, path: PathLike) -> "SticsCultivarFile":
        """Load a STICS file from *path*."""

        source = Path(path)
        lines = source.read_text(encoding="utf-8-sig").splitlines()
        if len(lines) % 2:
            raise ValueError(f"{source}: nombre impair de lignes ({len(lines)})")

        pairs = [(lines[index], lines[index + 1]) for index in range(0, len(lines), 2)]
        starts = [
            index
            for index, (parameter, _) in enumerate(pairs)
            if parameter.strip().casefold() == "codevar"
        ]
        if not starts:
            raise ValueError(f"{source}: aucun paramètre codevar")

        species = cls._pairs_to_parameters(pairs[: starts[0]], "espèce")
        cultivars: OrderedDict[str, Parameters] = OrderedDict()
        for position, start in enumerate(starts):
            end = starts[position + 1] if position + 1 < len(starts) else len(pairs)
            block = pairs[start:end]
            name = block[0][1]
            if name in cultivars:
                raise ValueError(f"{source}: cultivar dupliqué: {name}")
            cultivars[name] = cls._pairs_to_parameters(block[1:], f"cultivar {name}")

        return cls(species, cultivars, source)

    @staticmethod
    def _pairs_to_parameters(pairs: list[tuple[str, str]], section: str) -> Parameters:
        parameters: Parameters = OrderedDict()
        for parameter, value in pairs:
            if parameter in parameters:
                raise ValueError(f"paramètre dupliqué dans {section}: {parameter}")
            parameters[parameter] = value
        return parameters

    def _validate(self) -> None:
        if any(name.strip().casefold() == "codevar" for name in self.species_parameters):
            raise ValueError("codevar ne peut pas être un paramètre d'espèce")
        if not self.cultivars:
            raise ValueError("le fichier doit contenir au moins un cultivar")
        for cultivar, parameters in self.cultivars.items():
            if not cultivar:
                raise ValueError("le nom d'un cultivar ne peut pas être vide")
            if any(name.strip().casefold() == "codevar" for name in parameters):
                raise ValueError(f"codevar ne peut pas être un paramètre du cultivar {cultivar}")

    @property
    def cultivar_names(self) -> tuple[str, ...]:
        """Cultivar names, in file order."""

        return tuple(self.cultivars)

    def get_species_parameter(self, parameter: str) -> str:
        return self.species_parameters[parameter]

    def set_species_parameter(self, parameter: str, value: object, create: bool = False) -> None:
        if parameter not in self.species_parameters and not create:
            raise KeyError(f"paramètre d'espèce inconnu: {parameter}")
        if parameter.strip().casefold() == "codevar":
            raise ValueError("codevar est réservé")
        self.species_parameters[parameter] = str(value)

    def get_cultivar_parameter(
        self,
        cultivar: str,
        parameter: str,
        fallback_to_species: bool = False,
    ) -> str:
        """Return a cultivar value, optionally falling back to species data."""

        values = self.cultivars[cultivar]
        if parameter in values:
            return values[parameter]
        if fallback_to_species and parameter in self.species_parameters:
            return self.species_parameters[parameter]
        raise KeyError(f"paramètre {parameter!r} absent du cultivar {cultivar!r}")

    def set_cultivar_parameter(
        self,
        cultivar: str,
        parameters: Mapping[str, object],
        create: bool = False,
    ) -> None:
        """Update several parameters of *cultivar* in one operation.

        All parameter names are validated before values are changed, so a bad
        key cannot leave the cultivar only partially updated.
        """

        values = self.cultivars[cultivar]
        reserved = [
            parameter
            for parameter in parameters
            if parameter.strip().casefold() == "codevar"
        ]
        if reserved:
            raise ValueError("codevar est réservé")

        unknown = [parameter for parameter in parameters if parameter not in values]
        if unknown and not create:
            names = ", ".join(repr(parameter) for parameter in unknown)
            raise KeyError(f"paramètre(s) absent(s) du cultivar {cultivar!r}: {names}")

        for parameter, value in parameters.items():
            values[parameter] = str(value)

    def create_cultivar(
        self,
        name: str,
        default_cultivar: str,
        parameters: Optional[Mapping[str, object]] = None,
        allow_new_parameters: bool = False,
    ) -> Parameters:
        """Clone *default_cultivar*, apply overrides and append it to the file.

        Unknown override names raise ``KeyError`` by default, which protects
        against misspelled STICS parameters. Set ``allow_new_parameters`` to
        explicitly append parameters absent from the default cultivar.
        """

        if not name:
            raise ValueError("le nom du nouveau cultivar ne peut pas être vide")
        if name in self.cultivars:
            raise ValueError(f"le cultivar existe déjà: {name}")
        if default_cultivar not in self.cultivars:
            raise KeyError(f"cultivar par défaut inconnu: {default_cultivar}")

        new_values = self.cultivars[default_cultivar].copy()
        for parameter, value in (parameters or {}).items():
            if parameter not in new_values and not allow_new_parameters:
                raise KeyError(
                    f"paramètre {parameter!r} absent du cultivar par défaut "
                    f"{default_cultivar!r}"
                )
            if parameter.strip().casefold() == "codevar":
                raise ValueError("codevar est réservé; utilisez l'argument name")
            new_values[parameter] = str(value)

        self.cultivars[name] = new_values
        return new_values

    def write(self, path: Optional[PathLike] = None) -> Path:
        """Write the current data using STICS alternating CRLF lines."""

        destination = Path(path) if path is not None else self.path
        if destination is None:
            raise ValueError("un chemin de sortie est requis")

        pairs = list(self.species_parameters.items())
        for cultivar, parameters in self.cultivars.items():
            pairs.append(("codevar", cultivar))
            pairs.extend(parameters.items())

        destination.parent.mkdir(parents=True, exist_ok=True)
        content = "".join(f"{parameter}\r\n{value}\r\n" for parameter, value in pairs)
        destination.write_bytes(content.encode("utf-8"))
        self.path = destination
        return destination
