from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd

class SticsTempoparv6Converter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, tempoparfixv6, usmdir):
        file_name = "tempoparv6.sti"
        fileContent = tempoparfixv6
        # Exporter le fichier vers le répertoire spécifié
        self.write_file(usmdir, file_name, fileContent)



