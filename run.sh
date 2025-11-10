#!/bin/bash

# =========================================================
# CONFIGURATION
# =========================================================

# Fichier de données local (CONFIRMÉ SELON 'ls')
LOCAL_DATA_FILE="ventes_big_data_final.csv"
HDFS_INPUT_DIR="/input"

# Dossier HDFS parent pour les sorties de l'exercice (selon l'énoncé)
HDFS_OUTPUT_ROOT="/output"

# Dossiers locaux pour les livrables
LOCAL_OUTPUT_BASE="./livrables_UA2"

# Assurez-vous que les scripts Python ont les droits d'exécution
echo "Attribution des droits d'exécution aux scripts Python..."
# Utilisation des noms de fichiers confirmés :
chmod +x mapper_top10.py
chmod +x reducer_top10.py
chmod +x mapper_retour.py
chmod +x reducer_retour.py
chmod +x mapper_paiements.py
chmod +x reducer_paiements.py
chmod +x mapper_ca.py
chmod +x reducer_ca.py

# =========================================================
# ÉTAPE 1 : PRÉPARATION (HDFS)
# =========================================================

echo "--- ÉTAPE 1 : PRÉPARATION HDFS ---"

# Supprimer l'ancien dossier d'entrée HDFS
hdfs dfs -rm -r -f "$HDFS_INPUT_DIR" > /dev/null 2>&1

# Créer le répertoire d'entrée HDFS
hdfs dfs -mkdir -p "$HDFS_INPUT_DIR"

# Charger le fichier de données dans HDFS
echo "Chargement du fichier $LOCAL_DATA_FILE vers $HDFS_INPUT_DIR/..."
hdfs dfs -put -f "$LOCAL_DATA_FILE" "$HDFS_INPUT_DIR/"

# =========================================================
# ÉTAPE 2 : CA PAR PAYS ET MOIS (2.1 / 5.1)
# =========================================================

JOB_NAME="ca_pays_mois"
HDFS_OUTPUT_DIR="$HDFS_OUTPUT_ROOT/output_ca"
LOCAL_OUTPUT_DIR="$LOCAL_OUTPUT_BASE/output_ca"

echo -e "\n--- ÉTAPE 2 : EXÉCUTION DU JOB '$JOB_NAME' (CA par Pays/Mois - Livrable 5.1) ---"
mkdir -p "$LOCAL_OUTPUT_DIR"

# NETTOYAGE HDFS DE L'ANCIEN DOSSIER
echo "Nettoyage de $HDFS_OUTPUT_DIR sur HDFS..."
hdfs dfs -rm -r -f "$HDFS_OUTPUT_DIR"

# Exécuter Hadoop Streaming avec PYTHON3 explicitement (Correction Code 127)
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="CA Pays/Mois" \
    -files mapper_ca.py,reducer_ca.py \
    -mapper "python3 mapper_ca.py" \
    -reducer "python3 reducer_ca.py" \
    -input "$HDFS_INPUT_DIR/*" \
    -output "$HDFS_OUTPUT_DIR"

# Vérification et Post-traitement local
if hdfs dfs -test -f "$HDFS_OUTPUT_DIR/_SUCCESS"; then
    echo "Récupération du CA par Pays/Mois dans $LOCAL_OUTPUT_DIR/..."
    hdfs dfs -get "$HDFS_OUTPUT_DIR/part-00000" "$LOCAL_OUTPUT_DIR/ca_pays_mois.txt"
    echo "Aperçu du CA par Pays/Mois (Livrable 5.1 - 10 premières lignes) :"
    cat "$LOCAL_OUTPUT_DIR/ca_pays_mois.txt" | head -n 10
else
    echo "ATTENTION : Le Job '$JOB_NAME' a échoué. Le fichier de sortie n'existe pas."
fi

# =========================================================
# ÉTAPE 3 : TOP 10 PRODUITS (2.2 / 5.2)
# =========================================================

JOB_NAME="top10_produits"
HDFS_OUTPUT_DIR="$HDFS_OUTPUT_ROOT/output_top10"
LOCAL_OUTPUT_DIR="$LOCAL_OUTPUT_BASE/output_top10"

echo -e "\n--- ÉTAPE 3 : EXÉCUTION DU JOB '$JOB_NAME' (Top 10 - Livrable 5.2) ---"
mkdir -p "$LOCAL_OUTPUT_DIR"

# NETTOYAGE HDFS DE L'ANCIEN DOSSIER
echo "Nettoyage de $HDFS_OUTPUT_DIR sur HDFS..."
hdfs dfs -rm -r -f "$HDFS_OUTPUT_DIR"

# Exécuter Hadoop Streaming avec PYTHON3 explicitement
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="Top 10 Produits" \
    -D mapreduce.job.reduces=1 \
    -files mapper_top10.py,reducer_top10.py \
    -mapper "python3 mapper_top10.py" \
    -reducer "python3 reducer_top10.py" \
    -input "$HDFS_INPUT_DIR/*" \
    -output "$HDFS_OUTPUT_DIR"

# Vérification et Post-traitement local
if hdfs dfs -test -f "$HDFS_OUTPUT_DIR/_SUCCESS"; then
    echo "Récupération du résultat Top 10 dans $LOCAL_OUTPUT_DIR/..."
    hdfs dfs -get "$HDFS_OUTPUT_DIR/part-00000" "$LOCAL_OUTPUT_DIR/classement_top10.txt"
    echo "Aperçu du Top 10 (Livrable 5.2) :"
    cat "$LOCAL_OUTPUT_DIR/classement_top10.txt"
else
    echo "ATTENTION : Le Job '$JOB_NAME' a échoué. Le fichier de sortie n'existe pas."
fi

# =========================================================
# ÉTAPE 4 : TAUX DE RETOUR (2.3 / 5.3)
# =========================================================

JOB_NAME="taux_de_retour"
HDFS_OUTPUT_DIR="$HDFS_OUTPUT_ROOT/output_retours"
LOCAL_OUTPUT_DIR="$LOCAL_OUTPUT_BASE/output_retours"

echo -e "\n--- ÉTAPE 4 : EXÉCUTION DU JOB '$JOB_NAME' (Taux de Retour - Livrable 5.3) ---"
mkdir -p "$LOCAL_OUTPUT_DIR"

# NETTOYAGE HDFS DE L'ANCIEN DOSSIER
echo "Nettoyage de $HDFS_OUTPUT_DIR sur HDFS..."
hdfs dfs -rm -r -f "$HDFS_OUTPUT_DIR"

# Exécuter Hadoop Streaming avec PYTHON3 explicitement
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="Taux de Retour" \
    -D mapreduce.job.reduces=1 \
    -files mapper_retour.py,reducer_retour.py \
    -mapper "python3 mapper_retour.py" \
    -reducer "python3 reducer_retour.py" \
    -input "$HDFS_INPUT_DIR/*" \
    -output "$HDFS_OUTPUT_DIR"

# Vérification et Post-traitement local
if hdfs dfs -test -f "$HDFS_OUTPUT_DIR/_SUCCESS"; then
    echo "Récupération du Taux de Retour dans $LOCAL_OUTPUT_DIR/..."
    hdfs dfs -get "$HDFS_OUTPUT_DIR/part-00000" "$LOCAL_OUTPUT_DIR/taux_global.txt"
    echo "Aperçu du Taux de Retour (Livrable 5.3) :"
    cat "$LOCAL_OUTPUT_DIR/taux_global.txt"
else
    echo "ATTENTION : Le Job '$JOB_NAME' a échoué. Le fichier de sortie n'existe pas."
fi

# =========================================================
# ÉTAPE 5 : RÉPARTITION DES PAIEMENTS (2.4 / 5.4)
# =========================================================

JOB_NAME="repartition_paiements"
HDFS_OUTPUT_DIR="$HDFS_OUTPUT_ROOT/output_paiements"
LOCAL_OUTPUT_DIR="$LOCAL_OUTPUT_BASE/output_paiements"

echo -e "\n--- ÉTAPE 5 : EXÉCUTION DU JOB '$JOB_NAME' (Répartition Paiements - Livrable 5.4) ---"
mkdir -p "$LOCAL_OUTPUT_DIR"

# NETTOYAGE HDFS DE L'ANCIEN DOSSIER
echo "Nettoyage de $HDFS_OUTPUT_DIR sur HDFS..."
hdfs dfs -rm -r -f "$HDFS_OUTPUT_DIR"

# Exécuter Hadoop Streaming avec PYTHON3 explicitement
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="Répartition Paiements" \
    -D mapreduce.job.reduces=1 \
    -files mapper_paiements.py,reducer_paiements.py \
    -mapper "python3 mapper_paiements.py" \
    -reducer "python3 reducer_paiements.py" \
    -input "$HDFS_INPUT_DIR/*" \
    -output "$HDFS_OUTPUT_DIR"

# Vérification et Post-traitement local
if hdfs dfs -test -f "$HDFS_OUTPUT_DIR/_SUCCESS"; then
    echo "Récupération de la Répartition des Paiements dans $LOCAL_OUTPUT_DIR/..."
    hdfs dfs -get "$HDFS_OUTPUT_DIR/part-00000" "$LOCAL_OUTPUT_DIR/repartition_ca.txt"
    echo "Aperçu de la Répartition des Paiements (Livrable 5.4) :"
    cat "$LOCAL_OUTPUT_DIR/repartition_ca.txt"
else
    echo "ATTENTION : Le Job '$JOB_NAME' a échoué. Le fichier de sortie n'existe pas."
fi

echo -e "\n--- FIN DE L'EXÉCUTION ---"

# =========================================================
# ÉTAPE 6 : LANCEMENT DU DASHBOARD STREAMLIT
# =========================================================

echo ""
echo "--- 🚀 LANCEMENT DU DASHBOARD STREAMLIT ---"

# Dossier des livrables lu par dashboard.py
mkdir -p "$LOCAL_OUTPUT_BASE"/{output_ca,output_top10,output_retours,output_paiements}

# Lancer Streamlit en arrière-plan (port 8501 déjà publié dans docker-compose)
nohup streamlit run /root/dashboard.py \
  --server.address 0.0.0.0 \
  --server.port 8501 \
  > /root/streamlit.log 2>&1 &

sleep 2
echo "✅ Dashboard lancé sur http://localhost:8501"
echo "---- Dernières lignes de /root/streamlit.log ----"
tail -n 20 /root/streamlit.log

