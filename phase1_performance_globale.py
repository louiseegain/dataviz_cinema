import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
import os

# Configuration
DB_PATH = os.path.join(os.getcwd(), "concorde.db")

def charger_donnees():
    """Charge les données depuis la base SQLite"""
    
    if not os.path.exists(DB_PATH):
        print(" Erreur : Base de données non trouvée !")
        print(f"   Veuillez d'abord exécuter generate_data.py")
        print(f"   Chemin recherché : {DB_PATH}")
        return None
    
    print(" Chargement des données depuis la base...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Requête pour la performance globale par type de film
    query = """
    SELECT 
        f.type_film,
        COUNT(s.seance_id) as nb_seances,
        ROUND(AVG(s.taux_occupation), 1) as taux_occupation_moyen,
        SUM(s.nb_spectateurs) as spectateurs_total,
        ROUND(AVG(s.nb_spectateurs), 1) as spectateurs_moyen_par_seance
    FROM films f
    JOIN seances s ON f.film_id = s.film_id
    GROUP BY f.type_film
    ORDER BY taux_occupation_moyen DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f" Données chargées : {len(df)} types de films")
    return df

def creer_graphique(df):
    """Crée le graphique en barres horizontales avec sélection TOP 4"""
    
    # Configuration du style
    plt.style.use('seaborn-v0_8')
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Définir les couleurs : TOP 4 en vert, autres en gris
    colors = ['#27ae60' if i < 4 else '#95a5a6' for i in range(len(df))]
    
    # Créer le graphique en barres horizontales
    bars = ax.barh(df['type_film'], df['taux_occupation_moyen'], color=colors)
    
    # Ajouter les valeurs sur les barres
    for i, (bar, taux) in enumerate(zip(bars, df['taux_occupation_moyen'])):
        width = bar.get_width()
        ax.text(width + 1, bar.get_y() + bar.get_height()/2, 
                f'{taux:.1f}%', 
                ha='left', va='center', fontweight='bold', fontsize=10)
    
    # Personnalisation du graphique
    ax.set_xlabel('Taux d\'occupation moyen (%)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Type de film', fontsize=12, fontweight='bold')
    ax.set_title('PHASE 1 - Performance Globale par Type de Film\n' + 
                'Sélection des 4 types prioritaires pour Le Concorde', 
                fontsize=14, fontweight='bold', pad=20)
    
    # Ajuster les limites
    ax.set_xlim(0, max(df['taux_occupation_moyen']) * 1.15)
    
    # Légende
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='#27ae60', label='Top 4 sélectionnés'),
        Patch(facecolor='#95a5a6', label='Autres types')
    ]
    ax.legend(handles=legend_elements, loc='lower right', fontsize=10)
    
    # Ajustement de la mise en page
    plt.tight_layout()
    
    return fig

def afficher_analyse(df):
    """Affiche l'analyse détaillée des résultats"""
    
    print("\n" + "="*70)
    print(" ANALYSE - PHASE 1 : PERFORMANCE GLOBALE")
    print("="*70)
    
    print(f"\n CATALOGUE DU CONCORDE :")
    print(f"   • {len(df)} types de films programmés")
    print(f"   • {df['nb_seances'].sum():,} séances total")
    print(f"   • {df['spectateurs_total'].sum():,} spectateurs total")
    print(f"   • {df['taux_occupation_moyen'].mean():.1f}% taux moyen global")
    
    print(f"\n TOP 4 TYPES SÉLECTIONNÉS :")
    top_4 = df.head(4)
    for i, row in top_4.iterrows():
        print(f"   {i+1}. {row['type_film']:<25} {row['taux_occupation_moyen']:>6.1f}% "
              f"({row['nb_seances']:>3} séances)")
    
    print(f"\n PERFORMANCE DÉTAILLÉE TOP 4 :")
    for i, row in top_4.iterrows():
        print(f"\n   • {row['type_film']} :")
        print(f"     - Taux d'occupation : {row['taux_occupation_moyen']:.1f}%")
        print(f"     - Nombre de séances : {row['nb_seances']}")
        print(f"     - Spectateurs total : {row['spectateurs_total']:,}")
        print(f"     - Moyenne par séance : {row['spectateurs_moyen_par_seance']:.1f}")
    
    print(f"\n INSIGHTS CLÉS :")
    top_1 = df.iloc[0]
    dernier = df.iloc[-1]
    ecart = top_1['taux_occupation_moyen'] - dernier['taux_occupation_moyen']
    
    print(f"   • {top_1['type_film']} domine avec {top_1['taux_occupation_moyen']:.1f}%")
    print(f"   • Écart de {ecart:.1f} points entre le 1er et le dernier")
    print(f"   • Les 4 types sélectionnés représentent {top_4['nb_seances'].sum()} séances")
    print(f"   • Cohérent avec l'identité art et essai du Concorde")

def sauvegarder_resultats(df, fig):
    """Sauvegarde les résultats"""
    
    # Créer dossier de sortie
    output_dir = "analyses"
    os.makedirs(output_dir, exist_ok=True)
    
    # Sauvegarder le graphique
    fig.savefig(os.path.join(output_dir, 'phase1_performance_globale.png'), 
                dpi=300, bbox_inches='tight', facecolor='white')
    print(f"\n Graphique sauvegardé : {output_dir}/phase1_performance_globale.png")
    
    # Sauvegarder les données
    df.to_csv(os.path.join(output_dir, 'phase1_donnees.csv'), 
              index=False, encoding='utf-8-sig')
    print(f" Données sauvegardées : {output_dir}/phase1_donnees.csv")
    
    # Sauvegarder le TOP 4 pour les phases suivantes
    top_4 = df.head(4)
    top_4.to_csv(os.path.join(output_dir, 'top4_types_selectionnes.csv'), 
                 index=False, encoding='utf-8-sig')
    print(f" TOP 4 sauvegardé : {output_dir}/top4_types_selectionnes.csv")

def main():
    """Fonction principale"""
    
    print(" PHASE 1 - ANALYSE PERFORMANCE GLOBALE")
    print("="*50)
    
    # Chargement des données
    df = charger_donnees()
    if df is None:
        return
    
    # Création du graphique
    print(" Création du graphique...")
    fig = creer_graphique(df)
    
    # Analyse des résultats
    afficher_analyse(df)
    
    # Sauvegarde
    sauvegarder_resultats(df, fig)
    
    # Affichage du graphique
    print(f"\n  Affichage du graphique...")
    plt.show()
    
    print(f"\n PHASE 1 TERMINÉE !")
    print(f"   → 4 types prioritaires identifiés")
    print(f"   → Prêt pour la Phase 2 (test trimestre)")

if __name__ == "__main__":
    main()