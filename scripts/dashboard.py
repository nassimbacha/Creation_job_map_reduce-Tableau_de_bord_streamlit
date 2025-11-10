import streamlit as st

import pandas as pd

import plotly.express as px

import os



# --- CONFIGURATION ---

BASE_PATH = "./livrables_UA2"



# Titre du tableau de bord

st.set_page_config(layout="wide", page_title="Analyse Ventes MapReduce")

st.title("📊 Tableau de Bord d'Analyse des Ventes (Hadoop MapReduce)")

st.caption("Visualisation des Livrables 5.1 à 5.4 agrégés par MapReduce.")



# =========================================================

# UTILS : Fonction de chargement générique

# =========================================================



def load_data(file_name, sep='\t', header=None, names=None):

    """Charge un fichier CSV/TXT local et vérifie son existence."""

    file_path = os.path.join(BASE_PATH, file_name)

    if not os.path.exists(file_path):

        st.error(f"Fichier non trouvé: {file_path}. Veuillez vérifier l'exécution de run.sh.")

        return None

    try:

        df = pd.read_csv(file_path, sep=sep, header=header, names=names, skipinitialspace=True)

        return df

    except Exception as e:

        st.error(f"Erreur lors de la lecture de {file_path}. Le format est-il correct? Détail: {e}")

        return None



# =========================================================

# 1. LIVRABLE 5.1 : CA PAR PAYS ET MOIS

# =========================================================



st.header("1. 🌍 Chiffre d'Affaires Net par Pays et par Mois (Livrable 5.1)")

# Fichier: "YYYY-MM Pays<TAB>CA"
df_ca_brut = load_data("output_ca/ca_pays_mois.txt", sep=r"\s+", names=['Mois', 'Pays', 'CA Net'])

if df_ca_brut is not None:
    try:
        # Nettoyage chiffres
        df_ca_brut['CA Net'] = pd.to_numeric(df_ca_brut['CA Net'], errors='coerce')
        df_ca_brut = df_ca_brut.dropna(subset=['CA Net'])

        # (Option) filtre par mois
        mois_dispos = sorted(df_ca_brut['Mois'].unique())
        mois_sel = st.selectbox("Filtrer sur un mois (optionnel)", ["Tous"] + mois_dispos)
        df_src = df_ca_brut if mois_sel == "Tous" else df_ca_brut[df_ca_brut['Mois'] == mois_sel]

        # Barres verticales EMPILÉES : X = Pays, Y = CA, Couleur = Mois
        st.subheader("Ventes par Pays (Barres verticales empilées par Mois)")
        df_stack = (
            df_src.groupby(['Pays', 'Mois'], as_index=False)['CA Net'].sum()
            .sort_values(['Pays', 'Mois'])
        )

        # (Option) forcer l'ordre des mois
        mois_order = sorted(df_stack['Mois'].unique())
        df_stack['Mois'] = pd.Categorical(df_stack['Mois'], categories=mois_order, ordered=True)

        fig_bar_empile = px.bar(
            df_stack,
            x='Pays',
            y='CA Net',
            color='Mois',
            barmode='stack',  # empilé (pas la somme plate)
            title="Chiffre d'Affaires Net par Pays (empilé par Mois)",
            labels={'CA Net': "CA Net (€)", 'Pays': 'Pays', 'Mois': 'Mois'},
            template="plotly_white",
        )
        fig_bar_empile.update_yaxes(tickformat=",.0f")  # formatage 1 234 567

        st.plotly_chart(fig_bar_empile, use_container_width=True)

        # Tableau détaillé
        st.subheader("Tableau Détaillé (Pays x Mois)")
        st.dataframe(
            df_ca_brut[['Pays','Mois','CA Net']].sort_values(['Mois','Pays']).style.format({"CA Net": "{:,.2f} €"}),
            hide_index=True
        )

    except Exception as e:
        st.error(f"Erreur d'analyse des données CA Pays/Mois : {e}")

st.markdown("---")



# =========================================================

# 2. LIVRABLE 5.2 : TOP 10 PRODUITS

# =========================================================



st.header("2. 🥇 Top 10 Produits par Chiffre d'Affaires Net (Livrable 5.2)")



df_top10 = load_data("output_top10/classement_top10.txt", names=['Produit ID', 'CA Net'])



if df_top10 is not None:

    df_top10['CA Net'] = pd.to_numeric(df_top10['CA Net'], errors='coerce')

    df_top10 = df_top10.dropna(subset=['CA Net'])

    df_top10['CA Net (M€)'] = (df_top10['CA Net'] / 1_000_000).round(2)

    

    df_top10['Produit ID'] = df_top10['Produit ID'].astype(str)

    

    st.subheader("Comparaison des ID Produits et leur Chiffre d'Affaires")

    fig_top10 = px.bar(

        df_top10.sort_values(by='CA Net', ascending=False), 

        x='Produit ID', 

        y='CA Net', 

        orientation='v', 

        title='Classement des 10 produits (CA Net)',

        labels={'CA Net': "CA Net (€)", 'Produit ID': 'ID Produit'},

        template="plotly_white",

        color='CA Net',

        color_continuous_scale=px.colors.sequential.Agsunset,

    )

    st.plotly_chart(fig_top10, use_container_width=True)

    

    st.subheader("Tableau des Résultats (Produit ID / CA Net)")

    st.dataframe(

        df_top10.sort_values(by='CA Net', ascending=False)[['Produit ID', 'CA Net (M€)']].style.format({"CA Net (M€)": "{:.2f} M€"}), 

        hide_index=True

    )



st.markdown("---")



# =========================================================

# 3. LIVRABLE 5.3 : TAUX DE RETOUR GLOBAL

# =========================================================



st.header("3. 🔄 Taux de Retour Global (par Quantité) (Livrable 5.3)")



df_retours = load_data("output_retours/taux_global.txt", sep=':', names=['Metric', 'Value'])



if df_retours is not None and not df_retours.empty:

    df_retours['Metric'] = df_retours['Metric'].str.strip()

    df_retours['Value'] = df_retours['Value'].str.strip().str.replace(',', '', regex=False).str.replace('%', '', regex=False)



    try:

        total_sold = pd.to_numeric(df_retours.loc[df_retours['Metric'] == 'Quantité Totale Vendue', 'Value'].squeeze(), errors='coerce')

        total_returned = pd.to_numeric(df_retours.loc[df_retours['Metric'] == 'Quantité Totale Retournée', 'Value'].squeeze(), errors='coerce')

        taux = pd.to_numeric(df_retours.loc[df_retours['Metric'] == 'Taux de Retour Global (%)', 'Value'].squeeze(), errors='coerce')



        if pd.isna(total_sold) or pd.isna(total_returned) or pd.isna(taux):

            raise ValueError("L'une des métriques clés n'a pas pu être convertie.")



        col_met1, col_met2, col_met3 = st.columns(3)

        col_met1.metric("Quantité Totale Vendue", f"{int(total_sold):,}", delta_color="off")

        col_met2.metric("Quantité Totale Retournée", f"{int(total_returned):,}", delta_color="off")

        col_met3.metric("Taux de Retour Global", f"{taux:.2f}%", delta_color="off")



        df_distrib = pd.DataFrame({

            'Statut': ['Vendu (Net)', 'Retourné'],

            'Quantité': [total_sold - total_returned, total_returned]

        })



        fig_distrib = px.pie(

            df_distrib,

            values='Quantité',

            names='Statut',

            title='Distribution des Quantités (Retour vs Vente Nette)',

            color_discrete_sequence=['#28a745', '#dc3545']

        )

        st.plotly_chart(fig_distrib, use_container_width=True)



    except Exception as e:

        st.warning(f"Erreur lors de l'analyse des données de retour : {e}")

        st.dataframe(df_retours)

else:

    st.warning("Aucune donnée de retour trouvée dans le fichier taux_global.txt.")



st.markdown("---")



# =========================================================

# 4. LIVRABLE 5.4 : RÉPARTITION PAR PAIEMENT

# =========================================================



st.header("4. 💳 Répartition des Ventes par Mode de Paiement (Livrable 5.4)")



df_paiements = load_data("output_paiements/repartition_ca.txt", names=['Mode de Paiement', 'CA Net'])



if df_paiements is not None:

    df_paiements['CA Net'] = pd.to_numeric(df_paiements['CA Net'], errors='coerce')

    df_paiements = df_paiements.dropna(subset=['CA Net'])

    

    total_ca = df_paiements['CA Net'].sum()

    df_paiements['Pourcentage'] = (df_paiements['CA Net'] / total_ca) * 100

    df_paiements['CA Net (M€)'] = (df_paiements['CA Net'] / 1_000_000).round(2) 

    

    fig_pie = px.pie(

        df_paiements, 

        values='Pourcentage', 

        names='Mode de Paiement', 

        title='Part du CA Net par Mode de Paiement',

        hole=.5, 

        template="plotly_white"

    )

    st.plotly_chart(fig_pie, use_container_width=True)
