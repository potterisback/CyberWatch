# Démarrer CyberWatch et le mettre sur GitHub

Ce guide part de zéro sur Windows. Le code, le README et le rapport du projet
sont en anglais. Les commandes se lancent dans le dossier qui contient
**`run.py`, `README.md` et `docker-compose.yml`**.

## 1. Extraire et ouvrir le projet

1. Télécharge le ZIP puis fais clic droit → **Extraire tout**.
2. Ouvre le dossier `CyberWatch` extrait.
3. Dans VS Code : **File → Open Folder**, choisis ce dossier.
4. Ouvre **Terminal → New Terminal**. Si le terminal s'appelle PowerShell,
   c'est très bien.
5. Vérifie que tu es au bon endroit :

```powershell
dir
```

Tu dois voir `run.py`. Sinon, ouvre le bon dossier avant de continuer.

## 2. Voir une première démonstration

Installe Python **3.11 ou 3.12** depuis https://www.python.org/downloads/ si
nécessaire. Coche **Add Python to PATH**. Ferme et rouvre le terminal après
l'installation.

```powershell
python --version
python run.py demo
```

Si `python` n'est pas reconnu mais que le lanceur `py` existe :

```powershell
py -3.12 run.py demo
```

Ouvre **http://localhost:8000** dans ton navigateur. Le terminal doit rester
ouvert pendant que tu regardes le tableau de bord. Pour l'arrêter : **Ctrl+C**.

Cette première démonstration utilise des données synthétiques explicitement
signalées. Elle sert à vérifier le fonctionnement sans Docker ; ce n'est pas
la pile Big Data ni la mesure de précision du modèle.

## 3. Essayer le modèle sur les vraies données

Le modèle entraîné est déjà inclus. Tu peux l'utiliser sans installer
scikit-learn :

```powershell
python run.py download --mirror
python run.py ingest --file data/raw/test.csv --model artifacts/forest.json
python run.py serve
```

Ouvre à nouveau http://localhost:8000. Les résultats affichés viennent cette
fois des fichiers UNSW-NB15. Le téléchargement utilise un miroir public,
avec versions et empreintes vérifiées ; le détail est dans `docs/DATA.md`.

Pour réentraîner toi-même le modèle :

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-ml.txt
.\.venv\Scripts\python.exe run.py train
```

Utiliser directement le Python de `.venv` évite les problèmes PowerShell
d'activation de l'environnement.

## 4. Lancer la pile Big Data

1. Installe et démarre Docker Desktop : https://www.docker.com/products/docker-desktop/
2. Active son moteur Linux/WSL 2 si l'installation le demande.
3. Arrête le serveur Python précédent avec Ctrl+C pour libérer le port 8000.
4. Dans le terminal à la racine du projet :

```powershell
docker compose version
docker compose up -d --build
docker compose ps
```

Le premier lancement télécharge des images et des connecteurs. Prévois de la
place disque et environ 12 Go de RAM disponibles pour Docker. Sur un ordinateur
moins puissant, commence par les modes locaux.

Envoie ensuite des événements réels dans Kafka :

```powershell
docker compose run --rm runner python run.py download --mirror
docker compose run --rm runner python run.py produce --bootstrap kafka:29092 --file data/raw/test.csv --rate 300
```

Attends le premier micro-batch et ouvre http://localhost:8000. Le tableau de
bord interroge alors **Trino**, qui lit les tables **Iceberg** produites par
**Spark** à partir de **Kafka**. Ce n'est plus la base SQLite de la démo.

Pour voir les journaux ou vérifier la chaîne complète :

```powershell
docker compose logs --tail 80 stream
python scripts/query_trino.py
python scripts/stack_smoke.py
```

Le dernier test ajoute 40 enregistrements synthétiques identifiables. Il sert
au diagnostic ; ne les compte pas dans une évaluation scientifique.

Pour arrêter en conservant les données :

```powershell
docker compose down
```

## 5. Installer Git

Installe Git pour Windows : https://git-scm.com/downloads/win
Conserve **Git Credential Manager** pendant l'installation, puis rouvre VS Code.

```powershell
git --version
git config --global user.name "Harry Lebon-Barré"
git config --global user.email "TON_ADRESSE_LIEE_A_GITHUB"
```

Remplace la dernière valeur par ton adresse GitHub, ou l'adresse `noreply`
proposée dans les paramètres **Emails** de GitHub.

## 6A. Si le professeur t'a donné un dépôt / GitHub Classroom

**Utilise ce dépôt. N'en crée pas un autre pour le rendu.**

1. Accepte l'invitation GitHub Classroom, si nécessaire.
2. Sur le dépôt attribué, clique **Code → HTTPS → Copy**.
3. Place-toi dans un dossier parent, par exemple ton dossier de projets, puis :

```powershell
git clone URL_DU_DEPOT_DONNE_PAR_LE_PROF
cd NOM_DU_DOSSIER_CLONE
```

4. Copie **le contenu** de `CyberWatch` extrait dans ce dossier cloné :
   `README.md`, `run.py`, `cyberwatch`, `jobs`, `docker`, `docs`, etc.
   Copie aussi `.gitignore`, `.gitattributes`, `.dockerignore` et `.github`.
   Ne copie pas un éventuel autre dossier `.git` et ne supprime pas les
   consignes ou fichiers d'évaluation fournis par le professeur. Si un README
   de consignes existe déjà, garde-en une copie avant de mettre celui du projet.
5. Lance :

```powershell
git status
git add .
git commit -m "Add CyberWatch intrusion detection lakehouse"
git push
```

Ne lance pas `git init` ou `git remote add origin` dans un dépôt déjà cloné :
le lien avec GitHub existe déjà.

## 6B. Si tu dois créer ton propre dépôt

1. Sur https://github.com/new, choisis le nom **cyberwatch**.
2. Choisis la visibilité demandée par ton professeur.
3. Laisse décochées la création d'un README, d'un `.gitignore` et d'une licence :
   tu as déjà les fichiers du projet.
4. Clique **Create repository** et copie son URL HTTPS.
5. Dans VS Code, ouvre **le dossier CyberWatch extrait**, puis :

```powershell
git init -b main
git add .
git commit -m "Initial CyberWatch project"
git remote add origin https://github.com/TON_PSEUDO/cyberwatch.git
git push -u origin main
```

Remplace `TON_PSEUDO` et adapte le nom du dépôt si tu en as choisi un autre.
Git Credential Manager peut ouvrir ton navigateur : connecte-toi et autorise
Git. Ton mot de passe GitHub habituel ne fonctionne pas comme mot de passe
Git en HTTPS ; utilise la connexion navigateur proposée.

**N'envoie pas seulement le fichier ZIP.** Sur GitHub, on doit voir directement
le README, le code et les dossiers du projet.

## 7. Envoyer les modifications suivantes

Après avoir modifié des fichiers, dans le même dossier :

```powershell
git status
git add .
git commit -m "Describe the change made"
git push
```

| Commande | Signification |
|---|---|
| `git status` | Voir ce qui a changé |
| `git add .` | Préparer les fichiers à enregistrer |
| `git commit` | Enregistrer une version sur ton ordinateur |
| `git push` | Envoyer les versions enregistrées sur GitHub |
| `git pull --rebase` | Récupérer les versions distantes avant de continuer |

À plusieurs, commence une séance par `git pull --rebase` quand ton dossier de
travail est propre, puis fais tes modifications. Un `commit` sans `push`
reste uniquement sur ton ordinateur.

## 8. Vérifier le dépôt avant de rendre le lien

- Le README s'affiche et les fichiers sont accessibles.
- Le dépôt respecte le nom et la visibilité demandés.
- Ton professeur a accès au dépôt s'il est privé.
- L'onglet **Actions** affiche les tests. Le premier run installe Spark et peut
  prendre plusieurs minutes. Clique sur une étape rouge pour voir son erreur.
- `data/raw`, `.venv`, les bases locales et les checkpoints ne sont pas envoyés.
  C'est prévu : les scripts permettent de recréer ces éléments.
- Le lien à remettre est l'URL du dépôt, par exemple
  `https://github.com/TON_PSEUDO/cyberwatch`, sauf consigne différente.

## Dépannage Git

**`not a git repository`** : tu n'es pas dans le dossier du dépôt. Ouvre le bon
dossier ou suis 6B si aucun dépôt n'existe encore.

**`remote origin already exists`** : vérifie `git remote -v`. Si l'URL est celle
du dépôt voulu, passe à `git push`. Pour corriger une URL réellement erronée :

```powershell
git remote set-url origin URL_CORRECTE
```

**`src refspec main does not match any`** : vérifie qu'un premier `commit` existe
avec `git log -1` et regarde la branche avec `git branch`. Pour un nouveau dépôt
personnel, `git branch -M main` peut normaliser son nom après le premier commit.

**Push rejeté car le dépôt distant contient déjà des commits** : pour un dépôt
de cours existant, utilise le parcours 6A. N'utilise pas `push --force` pour
écraser le travail distant. Si tu as simplement pris du retard sur un dépôt
déjà cloné, termine/enregistre tes modifications puis `git pull --rebase`,
résous les conflits éventuels et recommence `git push`.

**Fichier trop volumineux** : vérifie que tu n'as pas ajouté un dataset, une
base ou un environnement virtuel. Le `.gitignore` fourni exclut ces éléments.

**Tu préfères une interface graphique** : GitHub Desktop permet de cloner le
dépôt, de faire **Commit to main**, puis **Push origin**. Les fichiers à copier
et le choix entre dépôt du professeur / dépôt personnel restent identiques.

Référence officielle :
https://docs.github.com/en/migrations/importing-source-code/using-the-command-line-to-import-source-code/adding-locally-hosted-code-to-github?platform=windows
