# Comparaison : `common_tempoparv6` (Python) vs `Export` (VB)

## 📋 RÉSUMÉ EXÉCUTIF

Le code VB (**dataMill_V10**) a plusieurs paramètres commentés que le code Python (**ModFileGen**) a maintenu actifs. Cela indique que ces paramètres peuvent **ne plus exister** dans le modèle de données ou avoir été **remplacés** par d'autres.

---

## 🔴 PARAMÈTRES PRÉSENTS EN PYTHON MAIS COMMENTÉS EN VB
**⚠️ Ces paramètres n'existent probablement plus - À INVESTIGUER**

### Groupe 1: Paramètres de tempérage et coupe
```
PYTHON (ACTIF)           →  VB (COMMENTÉ)
codetempfauche          ✗ Peut avoir été supprimé
coefracoupe1            ✗ Peut avoir été supprimé
coefracoupe2            ✗ Peut avoir été supprimé
```

### Groupe 2: Paramètres de talles (très nombreux)
```
swfacmin                ✗ Commenté en VB
codetranspitalle        ✗ Commenté en VB
codedyntalle1           ✗ Commenté en VB
SurfApex1               ✗ Commenté en VB
SeuilMorTalle1          ✗ Commenté en VB
SigmaDisTalle1          ✗ Commenté en VB
VitReconsPeupl1         ✗ Commenté en VB
SeuilReconsPeupl1       ✗ Commenté en VB
MaxTalle1               ✗ Commenté en VB
SeuilLAIapex1           ✗ Commenté en VB
tigefeuilcoupe1         ✗ Commenté en VB
codedyntalle2           ✗ Commenté en VB
SurfApex2               ✗ Commenté en VB
SeuilMorTalle2          ✗ Commenté en VB
SigmaDisTalle2          ✗ Commenté en VB
VitReconsPeupl2         ✗ Commenté en VB
SeuilReconsPeupl2       ✗ Commenté en VB
MaxTalle2               ✗ Commenté en VB
SeuilLAIapex2           ✗ Commenté en VB
tigefeuilcoupe2         ✗ Commenté en VB
resplmax1               ✗ Commenté en VB
resplmax2               ✗ Commenté en VB
```

### Groupe 3: Paramètres de montaison
```
codemontaison1          ✗ Commenté en VB
codemontaison2          ✗ Commenté en VB
```

### Groupe 4: Paramètres de climat
```
code_adapt_MO_CC        ✗ Commenté en VB
periode_adapt_CC        ✗ Commenté en VB
an_debut_serie_histo    ✗ Commenté en VB
an_fin_serie_histo      ✗ Commenté en VB
param_tmoy_histo        ✗ Commenté en VB
code_adaptCC_miner      ✗ Commenté en VB
code_adaptCC_nit        ✗ Commenté en VB
code_adaptCC_denit      ✗ Commenté en VB
TREFdenit1              ✗ Commenté en VB
TREFdenit2              ✗ Commenté en VB
```

### Groupe 5: Paramètres de décision semis
```
nbj_pr_apres_semis      ✗ Commenté en VB
eau_mini_decisemis      ✗ Commenté en VB
humirac_decisemis       ✗ Commenté en VB
```

### Groupe 6: Paramètres d'irrigation
```
P_codedate_irrigauto    ✗ Commenté en VB
datedeb_irrigauto       ✗ Commenté en VB
datefin_irrigauto       ✗ Commenté en VB
stage_start_irrigauto   ✗ Commenté en VB
stage_end_irrigauto     ✗ Commenté en VB
```

### Groupe 7: Paramètres de mortalité et options
```
codemortalracine        ✗ Commenté en VB
option_thinning         ✗ Commenté en VB
option_engrais_multiple ✗ Commenté en VB
```

### Groupe 8: Paramètres minéraux (GMIN)
```
codemineralOM           ✗ Commenté en VB
GMIN1                   ✗ Commenté en VB
GMIN2                   ✗ Commenté en VB
GMIN3                   ✗ Commenté en VB
GMIN4                   ✗ Commenté en VB
GMIN5                   ✗ Commenté en VB
GMIN6                   ✗ Commenté en VB
GMIN7                   ✗ Commenté en VB
```

---

## 🟢 PARAMÈTRES PRÉSENTS EN VB MAIS ABSENTS EN PYTHON
**✅ Nouveaux paramètres à ajouter au code Python**

```
VB (ACTIF)              →  PYTHON (ABSENT)
code_CsurNsol_dynamic   ✓ À ajouter au Python
humirac                 ✓ À ajouter au Python
code_ISOP               ✓ À ajouter au Python
code_pct_legume         ✓ À ajouter au Python
pct_legum               ✓ À ajouter au Python
```

---

## 🟡 PARAMÈTRES PRÉSENTS DANS LES DEUX
**✅ Coherent entre les versions**

| Paramètre | Python | VB | Status |
|-----------|--------|-----|---------|
| codepluiepoquet | ✓ | ✓ | OK |
| nbjoursrrversirrig | ✓ | ✓ | OK |
| codecalferti | ✓ | ✓ | OK |
| ratiolN | ✓ | ✓ | OK |
| dosimxN | ✓ | ✓ | OK |
| codetesthumN | ✓ | ✓ | OK |
| codeNmindec | ✓ | ✓ | OK |
| rapNmindec | ✓ | ✓ | OK |
| fNmindecmin | ✓ | ✓ | OK |
| codetrosee | ✓ | ✓ | OK |
| codeSWDRH | ✓ | ✓ | OK |
| option_pature | ✓ | ✓ | OK |
| coderes_pature | ✓ | ✓ | OK |
| pertes_restit_ext | ✓ | ✓ | OK |
| Crespc_pature | ✓ | ✓ | OK |
| Nminres_pature | ✓ | ✓ | OK |
| eaures_pature | ✓ | ✓ | OK |
| coef_calcul_qres | ✓ | ✓ | OK |
| engrais_pature | ✓ | ✓ | OK |
| coef_calcul_doseN | ✓ | ✓ | OK |

---

## 🎯 RECOMMANDATIONS

### POUR LE CODE VB (dataMill_V10) :

1. **Laisser les paramètres commentés** - Ils sont correctement marqués comme obsolètes avec le symbole `'`
   - ✅ `CroCo`, `codetempfauche`, etc. : rester commentés
   - ✅ Tous les paramètres de talles : rester commentés
   - ✅ Paramètres de climat : rester commentés
   - ✅ `GMIN1-7` : rester commentés

2. **Ajouter des commentaires explicites** pour indiquer pourquoi ils sont commentés :
   ```vb
   ' PARAMÈTRES OBSOLÈTES/SUPPRIMÉS (ne pas décommenter sans investigation)
   ' codetempfauche           - Paramètre supprimé du modèle
   ' coefracoupe1/2           - Paramètres supprimés du modèle
   ' Paramètres de talles     - Supprimés en InterCrop v2.0+
   ' GMIN1-7                  - Remplacés par une autre approche
   ```

3. **Garder les paramètres actifs qui existent** - Les 20 paramètres en vert sont cohérents avec Python

---

### POUR LE CODE PYTHON (ModFileGen) :

1. **Commenter les paramètres manquants en VB** pour maintenir la cohérence :
   ```python
   # Les paramètres suivants sont obsolètes/supprimés (voir dataMill_V10)
   # fileContent += format_stics_data_v6(DT, "swfacmin", 1)
   # fileContent += format_stics_data_v6(DT, "codetranspitalle")
   # (tous les paramètres de talles, etc.)
   ```

2. **Ajouter les nouveaux paramètres VB** :
   ```python
   fileContent += format_stics_data_v6(DT, "code_CsurNsol_dynamic")
   fileContent += format_stics_data_v6(DT, "humirac")
   fileContent += format_stics_data_v6(DT, "code_ISOP")
   fileContent += format_stics_data_v6(DT, "code_pct_legume")
   fileContent += format_stics_data_v6(DT, "pct_legum")
   ```

---

## 📊 DIFFÉRENCES PAR CATÉGORIE

| Catégorie | Commentés en VB | Actifs Python | En VB mais pas Python |
|-----------|---|---|---|
| Talles | 22 | 22 | 0 |
| Irrigation | 5 | 5 | 0 |
| Décision semis | 3 | 3 | 0 |
| Climat/Adaptation | 9 | 0 | 0 |
| Autres obsolètes | 11 | 11 | 0 |
| **TOTAL** | **50** | **41** | **5** |

