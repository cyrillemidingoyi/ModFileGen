# Repository guidance

Before starting implementation work, read `TODO.md` and identify any item
related to the user's request. Do not work on unrelated TODO items unless the
user asks.

After completing implementation work:

1. update the status of directly affected items in `TODO.md`;
2. add newly discovered follow-up work when it is concrete and actionable;
3. briefly remind the user of the most relevant remaining TODO item in the
   final response.

Do not repeat the entire TODO list in every response. Mention at most one or two
items that are relevant to the work just completed.

## Technical rules

- Run targeted tests after every converter modification.
- Never modify fixture databases without documenting the migration.
- Preserve compatibility with standard and successive simulations.
- Use `ParameterResolver` for model defaults and soil-specific overrides.

## Relevant Codex conversations

Use this index to reconnect implementation work with earlier discussions. The
thread ID is the stable local reference; the short title is used for readability.

- **Gérer Paramètres par idsoil**
  - Session: `01a0479c-6aaa-7cd3-858a-968b77266734`
  - ParameterResolver, préchargement et cache des sols
  - Surcharges STICS et stratégie de calcul de q0
  - Irrigation STICS datée relativement au semis et préchargée par lot
  - Surcharges génériques `fictec1`/`fictec2` par `(idMangt, SeasonOrder, PlantOrder)`
  - `interrang` est un paramètre STICS de gestion étendue, pas une colonne partagée de `CropManagement`
  - Surcharges génériques des paramètres de point par `idPoint`
  - L’albédo STICS reste une propriété physique de `Soil`, sans surcharge `paramsol.albedo`
  - Extension prévue vers DSSAT


When a conversation establishes a durable architectural decision or resolves a
significant bug, add its short title, thread ID, and a one-line topic summary to
this section. Do not add unrelated conversations merely because they started
from this repository's working directory.
