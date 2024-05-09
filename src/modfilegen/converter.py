import sqlite3
import os
import datetime
from pathlib import Path

class Converter:
    def __init__(self):
        self.usmString = ""
        self.usmId = ""
        self.nthreads = 0
        self.sstart = 0
        self.send = 0
        self.MasterInput_Connection = None
        self.ModelDictionary_Connection = None

    def Import(self, DirectoryPath, model):
        # Implement import logic here
        pass

    def export(self, DirectoryPath):
        # Implement export logic here
        pass


    def write_file(self, DirectoryPath, FileName, FileContent):
        """
        Writes the given content to a file in the specified directory.

        Args:
            DirectoryPath (str): Path to the directory where the file should be created.
            FileName (str): Name of the file to be written.
            FileContent (str): Content to be written to the file.
        """
        if not os.path.exists(DirectoryPath):
            Path(DirectoryPath).mkdir(parents=True, exist_ok=True)
            #os.makedirs(DirectoryPath)
        
        with open(os.path.join(DirectoryPath, FileName), "w") as outfile:
            outfile.write(FileContent)

    def format_item(self, Item):
        """
        Formats the given item based on its type.

        Args:
            Item: The item to be formatted.

        Returns:
            str: The formatted item.
        """
        if isinstance(Item, datetime.datetime):
            # Format DateTime as "dd/MM/yyyy"
            item_formatted = Item.strftime("%d/%m/%Y")
        elif isinstance(Item, float):
            # Format Single or Double by replacing comma with dot
            item_formatted = str(Item).replace(",", ".")
        elif Item is None:
            item_formatted = None
        else:
            item_formatted = str(Item)

        return item_formatted

    def format_item_lg(self, Item, Lg_Zone):
        """
        Formats the given item based on its type and adjusts its length.

        Args:
            Item: The item to be formatted.
            Lg_Zone (float): Desired length of the formatted item.

        Returns:
            str: The formatted item with adjusted length.
        """
        if isinstance(Item, datetime.datetime):
            # Format DateTime as "dd/MM/yyyy"
            item_formatted = Item.strftime("%d/%m/%Y")
        elif isinstance(Item, float):
            # Format Single or Double by replacing comma with dot
            item_formatted = str(Item).replace(",", ".")
        elif Item is None:
            item_formatted = None
        else:
            item_formatted = str(Item)

        lg = len(item_formatted)

        if isinstance(Item, (float, str)):
            while lg < Lg_Zone:
                item_formatted = f" {item_formatted}"
                lg += 1
        elif isinstance(Item, str):
            while lg < Lg_Zone:
                item_formatted = f"{item_formatted} "
                lg += 1

        return item_formatted
    def format_item_date(ItemDate):
        """
        verify date format stocked with YYDDD format

        Args:
            ItemDate (str): The date string in the format "YYDDD".

        Returns:
            str: The formatted date string.
        """
        if ItemDate == "-99":
            return ItemDate

        longueur = len(ItemDate)
        annee = ItemDate[:longueur - 3]
        quantieme = ItemDate[longueur - 3:]

        if int(annee) < 10:
            annee = f"0{annee[1:]}"

        if int(annee) % 4 == 0:
            if int(quantieme) > 366:
                errorFormatted = True
        else:
            if int(quantieme) > 365:
                errorFormatted = True

        itemFormatted = f"{annee}{quantieme}"
        return itemFormatted

