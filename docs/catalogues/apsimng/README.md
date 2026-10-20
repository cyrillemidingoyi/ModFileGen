# Dictionnaire agronomique APSIM Next Generation

[Variables_APSIMNG.csv](Variables_APSIMNG.csv) conserve les **22 colonnes, dans
le même ordre**, de `Variables` dans
`tests/stics_successive/ModelsDictionaryArise.db`. Cette base a été ouverte
uniquement pour lire son schéma. Les anciennes lignes `Apsim` n'ont pas été
utilisées et aucune base n'a été modifiée.

Le fichier contient **1 403 variables : 576 entrées et 827 sorties**, regroupées
par catégories fonctionnelles communes aux cultures. Les descriptions sont
conservées dans leur langue d'origine, généralement l'anglais.

## Sens de Table et Champ

`Table` est une catégorie agronomique, jamais un namespace, une classe .NET,
une culture ou un chemin de ressource.

| Table | Champ, par exemple | Contenu |
|---|---|---|
| `physical` | `SAT`, `BD`, `ParticleSizeSand` | Propriétés physiques du sol |
| `organic` | `Carbon`, `SoilCNRatio`, `FOMCNRatio` | Matière organique du sol |
| `chemical` | `PH`, `EC`, `CEC` | Propriétés chimiques du sol |
| `soilwater` | `SummerU`, `WinterU`, `SWCON` | Paramètres du bilan hydrique |
| `initialwater`, `solute`, `soilcrop` | `InitialValues`, `InitialValuesUnits`, `KL` | État initial et propriétés sol–culture |
| `soil`, `soiltemperature` | `Latitude`, `InitialValues` | Métadonnées physiques et température du sol |
| `sowing` | `Population`, `Depth`, `RowSpacing` | Semis |
| `plant`, `phenology`, `structure` | Paramètres de développement | Paramétrage commun des plantes |
| `leaf`, `root`, `organ`, `reproductiveorgan` | `CarbonConcentration`, `MaximumNConc` | Propriétés des organes |
| `photosynthesis`, `transpiration`, `photoperiod` | `RUE`, `Twilight` | Paramètres physiologiques |
| `irrigation`, `fertiliser`, `fertilisertype` | `Amount`, `Depth` | Opérations et types d'apports |
| `surfaceorganicmatter`, `microclimate` | Paramètres de surface et du couvert | Résidus et microclimat |
| `residuetype` | `fraction_C`, `fr_c`, `fr_n`, `pot_decomp_rate` | Composition et décomposition des résidus |
| `tillage` | `Fraction`, `Depth`, `TillageDate`, `f_incorp`, `tillage_depth` | Travail du sol et incorporation des résidus |
| `autosowing` | `MinESW`, `MinRain`, `RainDays`, `MustSow` | Fenêtre et critères du semis automatique |
| `autoirrigation` | `FASWThreshold`, `FASWDepth`, `weeks`, `afterSowing` | Seuils, profondeur et délais d'irrigation |
| `autofertiliser` | `CritNThreshold`, `Threshold`, `TriggerValue` | Déclenchement conditionnel des apports |
| `organicfertiliser` | `DryMatterContent`, `ConcTotalN`, `SlurryAmount` | Apports organiques et leur composition |
| `reset`, `emergence`, `harvest` | `ResetWater`, `EmergenceDate`, `Crop` | Paramètres déclarés des règles correspondantes |
| `weather`, `clock` | `FileName`, `Start`, `End` | Configuration météo et période |
| `outputs` | `leaf.LAI`, `root.Depth`, `organic.Nitrogen` | Résultats et états calculés |

Les autres catégories détaillées sont `leafcohort`, `shoot`, `nodule`,
`nutrient`, `organicpool` et `volatilisation`, lorsqu'une variable admissible
est présente. Les effectifs exacts par catégorie figurent dans
[manifest.json](manifest.json).

Pour les entrées, `Champ` conserve le nom natif du paramètre. Pour les sorties,
le préfixe fonctionnel précise le contexte : `leaf.LAI` reste indépendant de
la culture, et `root.Depth` se distingue d'une profondeur de semis. Il s'agit
d'identifiants du dictionnaire : le chemin exact dans `Report` doit être adapté
aux noms des composants de la simulation, par exemple `[Leaf].LAI`.

Les paramètres scalaires définis par un enfant `Constant` sont nommés d'après
leur **dépendance biologique documentée**, par exemple `leaf.CarbonConcentration`,
et non `FixedValue`. Leur présence est reconnue dans les ressources de culture,
mais leur valeur propre à une culture n'est pas recopiée comme défaut générique.
Le nom de la phase peut préciser un paramètre de phénologie partagé, sans nommer
une culture.

## Sélection et regroupement

Le script sélectionne explicitement les composants agronomiques du sol,
du climat, de la gestion et des cultures génériques PMF/AgPasture. La liste de
classes et leur catégorie sont déclarées dans `CATEGORIES` du générateur.
Elle ne cherche plus à transformer toute l'API publique d'APSIM en dictionnaire.

Sont exclus : parseurs (`TSDMLParser`, `TTypedValue`…), interfaces techniques,
objets du moteur, dépendances `Link` comme lignes autonomes, métadonnées,
événements comme membres du moteur, méthodes, visualisation, commandes de cultivar,
chemins propres aux cultures et câblage interne des fonctions (`XYPairs`, événements d'accumulation).
En revanche, `VariableName` et `EventName` restent des paramètres légitimes
lorsqu'ils sont explicitement proposés par une règle de gestion conditionnelle.

Les variantes de casse sont regroupées comme dans ParameterResolver.
Les noms répétés dans plusieurs implémentations sont regroupés par catégorie
et variable lorsque le type et l'unité sont compatibles. Les références de
chaque définition restent dans `commentaire.sources`. Les différences de
notation `g/m2` et `g/m^2`, par exemple, sont normalisées sans conversion de valeur.

**Un homonyme n'est pas automatiquement une même variable.** Six homonymes
avec types ou unités incompatibles sont désambiguïsés : `KNH4`, `KNO3`,
`organ.Wt`, `organ.N`, `StartStage` et la dose `Amount` des apports organiques. Leur nom conserve alors le contexte de
l'implémentation générique, par exemple `organ.GenericOrgan.N` en g/m2 et
`organ.Organ.N` en g. Aucune culture n'est ajoutée au nom. Le manifeste donne
la liste de ces distinctions ; aucune ligne fusionnée ne garde de conflit de
type ou d'unité.

Un setter public n'implique pas à lui seul une entrée : les getters calculés
sans indication d'édition, les champs d'état et les propriétés `JsonIgnore`
restent des sorties. Les propriétés de configuration et les paramètres
biologiques documentés sont des entrées. Certaines valeurs initiales sont
à la fois configurables et consultables ; la classification sert à distinguer
la configuration des états calculés, pas à garantir que toute propriété peut
être injectée directement dans le JSON.

Les cas vérifiés dans le code d'exécution prennent explicitement priorité sur
les règles automatiques (`VERIFIED_ROLES` dans le générateur) :

- `structure.FinalLeafNo` est une sortie calculée et transmise aux tiges/talles,
  y compris lorsque la propriété interne de `Culm` a un setter public ;
- `volatilisation.pH` est une sortie dynamique, initialisée depuis `chemical.PH`
  puis mise à jour par les processus azotés ; `chemical.PH` reste une entrée.

Ces décisions et leurs références aux affectations du code APSIM sont
conservées dans `commentaire.sources[].classification_review`. Les deux fausses
entrées ont été supprimées. Les autres propriétés modifiables nécessitent
encore une revue sémantique avant utilisation comme paramètres d'entrée.

## Descriptions, défauts et provenance

Les descriptions génériques peuvent être précisées après vérification du code
du processus (`VERIFIED_METADATA` dans le générateur). La description originale
reste dans `commentaire.sources`, et la clarification et ses références dans
`commentaire.metadata_review`. Ainsi, `tillage.cn_rain` désigne le seuil de
pluie cumulée après travail du sol, en mm, auquel la réduction du Curve Number
devient nulle ; la réduction initiale `cn_red` décroît linéairement avec ce cumul.
Dans WaterBalance, le cumul utilise `PotentialRunoff`, qui inclut également les
irrigations portant `WillRunoff`.

| Colonnes | Règle |
|---|---|
| `model` | `apsim` |
| `status` | `input` ou `output` |
| `domain` | Catégorie fonctionnelle ; contexte de la sortie pour `outputs` |
| `Description` | Documentation `Description` ou commentaire XML `summary` |
| `unit`, `Minnval`, `Maxval` | Attributs explicites `Units` et `Bounds` |
| `Type` | Type natif de la donnée, y compris les tableaux |
| `defaultvalueYN` | `1` seulement lorsqu'un défaut littéral générique est établi |
| `defaultValueOtherSource` | Ce défaut explicite, jamais une valeur prise arbitrairement dans une culture |
| `SourceDV` | Lien vers la définition de ce défaut, fixé à la révision source |
| `commentaire` | JSON contenant les définitions et la provenance du regroupement |
| `SuiviModif` | Révision source et date d'extraction |

Les colonnes de correspondance ACME/MasterInput, `Default_Value_Datamill` et
`FileNativeF` restent vides en l'absence d'une correspondance explicitement
établie. Les anciennes données APSIM de la base ne sont pas réutilisées.

Le catalogue fournit 1 365 descriptions et 46 défauts génériques explicites.
Une cellule vide signifie que l'information n'est pas établie. Les défauts
sont extraits des initialiseurs C# littéraux ou de `DefaultValue` ; un défaut
n'est conservé après regroupement que si toutes les déclarations concernées
s'accordent. Les valeurs implicites du langage, les constructeurs, les calculs
et les valeurs propres à une culture ne sont pas exécutés ni généralisés.
Les sorties n'ont pas de défaut de configuration.

Ainsi, `soilwater.SummerU` a un défaut explicite de `6`, `SummerCona` de `3.5`
et `WinterCona` de `2.5`. `organic.FOMCNRatio` n'a pas de défaut littéral dans
la classe et reste vide ici. Cela ne remet pas une valeur arbitraire dans
le convertisseur.

## Paramètres des règles de gestion

Les propriétés d'entrée et les variables publiques des **23 règles sélectionnées**
sont extraites de la [Management Toolbox officielle](https://github.com/APSIMInitiative/ApsimX/blob/9ca9e659bf6f10db9a17dc14072a50f9b8c36d13/ApsimNG/Resources/Toolboxes/ManagementToolbox.apsimx).
Le nom d'une entrée est vérifié dans la liste `Parameters` de la règle et sa
déclaration C#. Les autres données publiques de la règle sont des variables,
classées dans `outputs`, par exemple `autoirrigation.FASW` et
`autoirrigation.WaterDeficit`. Aucun appel à `Sow`, `Apply`, `Harvest` ou autre
méthode n'est ajouté comme ligne.

Les règles couvrent notamment : semis daté ou conditionnel, irrigation datée
ou automatique (déficit, allocation saisonnière, pivot, sillons), fertilisation
datée ou conditionnelle, incorporation et apports de résidus organiques,
et réinitialisation des états. Leur liste et les paramètres reconnus sont dans
`manifest.management_rules`. Ce sont les règles officielles retenues, pas un
ensemble universel de paramètres obligatoire pour tout script personnalisé.

Les références de culture telles que `Crop` sont des paramètres de sélection
du modèle, pas des entrées distinctes pour chaque culture. Leur type natif
(`IPlant`, par exemple) est conservé sans exporter l'interface comme catégorie.

Les valeurs enregistrées dans la toolbox sont des exemples de configuration.
Elles figurent seulement dans `commentaire.sources[].toolbox_example`, lorsque
pertinent ; elles ne deviennent pas des défauts génériques. Les noms de cultivar
et références à des modèles ne sont pas repris comme défauts. Les défauts C#
littéraux explicitement déclarés restent soumis à la règle de concordance.
Les unités écrites explicitement dans `Description`, par exemple `(mm)` ou
`(kg/ha)`, peuvent compléter l'attribut `Units` pour les paramètres Manager.
Une unité ou description non établie reste vide.

## Références et vérification

- [Documentation officielle APSIM NG](https://docs.apsim.info/docs/).
- [Sources officielles à la révision 9ca9e659bf6f10db9a17dc14072a50f9b8c36d13](https://github.com/APSIMInitiative/ApsimX/tree/9ca9e659bf6f10db9a17dc14072a50f9b8c36d13/Models),
  du 16 octobre 2026 ; extraction du 18 octobre 2026.
- [Licence du dépôt APSIM](https://github.com/APSIMInitiative/ApsimX/blob/9ca9e659bf6f10db9a17dc14072a50f9b8c36d13/LICENSE.md).

Les références de classes servent uniquement à documenter les sources, pas à
nommer les catégories. Le générateur lit 1 177 fichiers C#, 101 ressources et la toolbox de gestion,
puis applique la sélection agronomique. Les 23 règles ont été analysées sans
erreur et leurs paramètres déclarés ont tous été retrouvés. Les six fichiers présentant des limites
d'analyse C# sont hors du périmètre retenu. Aucun fichier contribuant au
catalogue ne présente d'erreur d'analyse. Les anciens membres de ressources
non résolus ne sont pas conservés.

Les contrôles portent sur les 22 colonnes, les clés uniques, la provenance,
les catégories, l'exclusion des types techniques et cultures, les métadonnées
du sol et la séparation des homonymes. Le manifeste fournit l'empreinte SHA-256.
Le catalogue n'a pas été importé dans une base. Sa couverture et les lignes
à utiliser par le convertisseur restent à valider sur une simulation de référence.
Les scripts personnalisés et les paramètres dynamiques ne constituent pas un
ensemble universel de variables communes aux cultures.

## Régénération

CSV en UTF-8 avec BOM, séparateur virgule. Générateur :
[build_apsimng_catalog.py](../../../tools/build_apsimng_catalog.py).
Dépendances de génération seulement : `tree-sitter==0.25.2` et
`tree-sitter-c-sharp==0.23.5`. Le code APSIM n'est pas exécuté.

```bash
python tools/build_apsimng_catalog.py \
  --apsim-source /tmp/modfilegen-apsimng-reference \
  --dictionary-schema tests/stics_successive/ModelsDictionaryArise.db \
  --output-dir docs/catalogues/apsimng \
  --extracted-on 2026-10-18
```

La copie source doit également contenir
`ApsimNG/Resources/Toolboxes/ManagementToolbox.apsimx`, à la même révision que
`Models`. Le générateur vérifie que ces sources ne sont pas modifiées.
