CREATE TABLE [Variables] (
[model] TEXT,
[Table] TEXT,
[Champ] TEXT,
[Description] TEXT,
[unit] TEXT,
[Type] TEXT,
[domain] TEXT,
[TableUsedInACMEYN] INTEGER,
[TableListYN] INTEGER,
[defaultvalueYN] INTEGER,
[status] TEXT,
[ACMEinputEquivalentYN] INTEGER,
[ACMEinputField] TEXT,
[FunctionACME] TEXT,
[Default_Value_Datamill] TEXT,
[FileNativeF] TEXT,
[defaultValueOtherSource] TEXT,
[SourceDV] TEXT,
[Minnval] REAL,
[Maxval] REAL,
[commentaire] TEXT,
[SuiviModif] TEXT
);

CREATE INDEX idx_model_table ON Variables (model, [Table]);
