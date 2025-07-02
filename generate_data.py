import sqlite3
import random
import pandas as pd
from datetime import datetime, timedelta
import os

# Configuration
DB_PATH = os.path.join(os.getcwd(), "concorde.db")

# Données de base
SALLES_CONFIG = [
    (1, 'Salle 1', 71),
    (2, 'Salle 2', 80),
    (3, 'Salle 3', 60),
    (4, 'Salle 4', 75)
]

TYPES_FILMS = [
    'Fiction française', 'Fiction étrangère', 'Documentaire', 'Animation',
    'Cinéma d\'auteur', 'Comédie', 'Drame', 'Thriller', 'Historique', 'Biopic'
]

CRENEAUX = ['09h00', '11h30', '14h00', '16h30', '18h30', '20h30', '22h00']
JOURS_SEMAINE = ['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']
TRANCHES_AGE = ['Moins de 14', 'Moins de 26', 'Tarif normal (26-60)', 'Senior (60+)']
VERSIONS = ['VO', 'VF', 'VOSTFR']

# Titres de films par genre (pour plus de réalisme)
TITRES_FILMS = {
    'Fiction française': [
        'Le Fabuleux Destin d\'Amélie Poulain', 'Intouchables', 'La Vie d\'Adèle',
        'Les Parapluies de Cherbourg', 'Huit Femmes', 'Le Prénom', 'Welcome',
        'Polisse', 'La Famille Bélier', 'Mesrine', 'Un Prophète', 'Entre les Murs',
        'La Haine', 'Les Choristes', 'Taxi Driver', 'Bienvenue chez les Ch\'tis',
        'Le Dîner de Cons', 'OSS 117', 'Astérix et Obélix', 'Les Visiteurs'
    ],
    'Fiction étrangère': [
        'Parasite', 'La Dolce Vita', 'Lost in Translation', 'Her',
        'The Grand Budapest Hotel', 'Midnight in Paris', 'Call Me by Your Name',
        'Roma', 'Moonlight', 'The Tree of Life', 'Drive', 'Blade Runner 2049',
        'The Shape of Water', 'Nomadland', 'Marriage Story', 'Pulp Fiction',
        'The Godfather', 'Casablanca', 'Citizen Kane', 'Vertigo'
    ],
    'Documentaire': [
        'Être et Avoir', 'Demain', 'Super Size Me', 'An Inconvenient Truth',
        'March of the Penguins', 'Bowling for Columbine', 'Won\'t You Be My Neighbor?',
        'Free Solo', 'The Act of Killing', 'Citizenfour', 'Amy', 'Faces Places',
        'The Cove', 'Blackfish', 'Food Inc', 'Sicko', 'Fahrenheit 9/11',
        'Woodstock', 'Grey Gardens', 'Dont Look Back'
    ],
    'Animation': [
        'Mon Voisin Totoro', 'Coco', 'Spirited Away', 'Inside Out',
        'WALL-E', 'Finding Nemo', 'Up', 'Toy Story', 'The Incredibles',
        'Ratatouille', 'Moana', 'Frozen', 'Zootopia', 'Big Hero 6',
        'How to Train Your Dragon', 'Shrek', 'The Lion King', 'Beauty and the Beast',
        'Akira', 'Princess Mononoke'
    ],
    'Cinéma d\'auteur': [
        'Amour', 'The Square', 'Melancholia', 'Antichrist',
        'Holy Motors', 'Uncle Boonmee', 'A Separation', 'Leviathan',
        'The Hunt', 'Force Majeure', 'Winter Sleep', 'Nymphomaniac',
        'Only God Forgives', 'The Lobster', 'Dogtooth', 'Alps',
        'Blue Is the Warmest Color', 'Shame', 'Hunger', 'Fish Tank'
    ],
    'Comédie': [
        'The Grand Budapest Hotel', 'In Bruges', 'Kiss Kiss Bang Bang',
        'The Nice Guys', 'Knives Out', 'Birdman', 'The Menu',
        'Everything Everywhere All at Once', 'Jojo Rabbit', 'Hunt for the Wilderpeople',
        'What We Do in the Shadows', 'Thor: Ragnarok', 'The Other Guys',
        'Anchorman', 'Zoolander', 'Meet the Parents', 'Wedding Crashers',
        'Superbad', 'Pineapple Express', 'This Is the End'
    ],
    'Drame': [
        'Manchester by the Sea', 'Room', 'Spotlight', 'The Revenant',
        'Birdman', '12 Years a Slave', 'Her', 'Gravity',
        'Life of Pi', 'The Artist', 'Black Swan', 'Inception',
        'No Country for Old Men', 'There Will Be Blood', 'Atonement',
        'Little Miss Sunshine', 'Crash', 'Million Dollar Baby', 'Mystic River',
        'The Hours'
    ],
    'Thriller': [
        'Gone Girl', 'Zodiac', 'Se7en', 'Fight Club',
        'The Social Network', 'Prisoners', 'Nightcrawler', 'Ex Machina',
        'Shutter Island', 'Memento', 'The Prestige', 'Following',
        'Insomnia', 'Heat', 'Collateral', 'The Departed',
        'Goodfellas', 'Casino', 'Scarface', 'Taxi Driver'
    ],
    'Historique': [
        'Gladiator', 'Braveheart', 'The Last Samurai', 'Kingdom of Heaven',
        'Troy', 'Alexander', '300', 'Master and Commander',
        'The Patriot', 'We Were Soldiers', 'Black Hawk Down',
        'Saving Private Ryan', 'Band of Brothers', 'The Pacific',
        'Dunkirk', '1917', 'They Shall Not Grow Old', 'Apocalypse Now',
        'Platoon', 'Full Metal Jacket'
    ],
    'Biopic': [
        'Bohemian Rhapsody', 'Rocketman', 'The Theory of Everything',
        'The Imitation Game', 'Steve Jobs', 'Lincoln', 'Gandhi',
        'Malcolm X', 'Ray', 'Walk the Line', 'La Vie en Rose',
        'The Queen', 'The Iron Lady', 'Darkest Hour',
        'Churchill', 'The King\'s Speech', 'Elizabeth', 'Marie Antoinette',
        'Napoleon', 'Alexander'
    ]
}

def creer_base_donnees():
    """Crée la base de données et les tables"""
    
    # Créer le dossier s'il n'existe pas
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    # Supprimer la DB existante si elle existe
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Création des tables
    cursor.execute('''
        CREATE TABLE films (
            film_id INTEGER PRIMARY KEY,
            titre TEXT,
            type_film TEXT,
            duree_minutes INTEGER,
            version TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE salles (
            salle_id INTEGER PRIMARY KEY,
            nom_salle TEXT,
            capacite INTEGER
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE seances (
            seance_id INTEGER PRIMARY KEY,
            film_id INTEGER,
            salle_id INTEGER,
            date_seance DATE,
            creneau TEXT,
            jour_semaine TEXT,
            nb_spectateurs INTEGER,
            taux_occupation REAL,
            FOREIGN KEY (film_id) REFERENCES films(film_id),
            FOREIGN KEY (salle_id) REFERENCES salles(salle_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE spectateurs (
            spectateur_id INTEGER PRIMARY KEY,
            seance_id INTEGER,
            tranche_age TEXT,
            FOREIGN KEY (seance_id) REFERENCES seances(seance_id)
        )
    ''')
    
    conn.commit()
    return conn

def generer_films():
    """Génère 100 films réalistes"""
    films = []
    film_id = 1
    
    for type_film in TYPES_FILMS:
        titres_disponibles = TITRES_FILMS[type_film].copy()
        
        # Nombre de films par type (répartition réaliste pour art et essai)
        nb_films_type = {
            'Fiction française': 15,
            'Fiction étrangère': 20,  # Plus pour art et essai
            'Documentaire': 15,       # Beaucoup pour art et essai
            'Animation': 10,
            'Cinéma d\'auteur': 12,
            'Comédie': 8,
            'Drame': 10,
            'Thriller': 6,
            'Historique': 4,
            'Biopic': 5
        }
        
        for i in range(nb_films_type[type_film]):
            if titres_disponibles:
                titre = titres_disponibles.pop(random.randint(0, len(titres_disponibles)-1))
            else:
                titre = f"{type_film} {i+1}"
            
            duree = random.randint(85, 180)  # Durée réaliste
            
            # Version selon le type (plus de VO pour fiction étrangère et auteur)
            if type_film in ['Fiction étrangère', 'Cinéma d\'auteur']:
                version = random.choices(['VO', 'VOSTFR'], weights=[70, 30])[0]
            elif type_film == 'Animation':
                version = random.choices(['VF', 'VO'], weights=[80, 20])[0]
            else:
                version = random.choices(['VF', 'VO'], weights=[85, 15])[0]
            
            films.append((film_id, titre, type_film, duree, version))
            film_id += 1
    
    return films

def generer_seances(conn):
    """Génère 3000 séances sur 6 mois"""
    
    # Récupérer les films et salles
    cursor = conn.cursor()
    cursor.execute("SELECT film_id, type_film FROM films")
    films = cursor.fetchall()
    
    seances = []
    seance_id = 1
    
    # Période : 6 mois (180 jours)
    date_debut = datetime(2024, 7, 1)
    
    # Patterns de performance par type de film et créneau (réalistes pour art et essai)
    patterns_performance = {
        'Fiction française': {'09h00': 25, '11h30': 35, '14h00': 55, '16h30': 68, '18h30': 82, '20h30': 88, '22h00': 42},
        'Fiction étrangère': {'09h00': 15, '11h30': 28, '14h00': 45, '16h30': 62, '18h30': 85, '20h30': 95, '22h00': 58},
        'Documentaire': {'09h00': 12, '11h30': 22, '14h00': 35, '16h30': 48, '18h30': 78, '20h30': 65, '22h00': 28},
        'Animation': {'09h00': 65, '11h30': 85, '14h00': 92, '16h30': 95, '18h30': 58, '20h30': 42, '22h00': 25},
        'Cinéma d\'auteur': {'09h00': 8, '11h30': 18, '14h00': 32, '16h30': 45, '18h30': 72, '20h30': 85, '22h00': 48},
        'Comédie': {'09h00': 18, '11h30': 32, '14h00': 48, '16h30': 62, '18h30': 75, '20h30': 82, '22h00': 35},
        'Drame': {'09h00': 12, '11h30': 25, '14h00': 38, '16h30': 52, '18h30': 68, '20h30': 78, '22h00': 38},
        'Thriller': {'09h00': 8, '11h30': 18, '14h00': 28, '16h30': 42, '18h30': 65, '20h30': 88, '22h00': 65},
        'Historique': {'09h00': 15, '11h30': 28, '14h00': 42, '16h30': 58, '18h30': 68, '20h30': 72, '22h00': 32},
        'Biopic': {'09h00': 12, '11h30': 25, '14h00': 38, '16h30': 55, '18h30': 72, '20h30': 78, '22h00': 35}
    }
    
    # Bonus week-end
    bonus_weekend = {'Samedi': 1.15, 'Dimanche': 1.10, 'Vendredi': 1.08}
    
    for jour in range(180):  # 6 mois
        date_courante = date_debut + timedelta(days=jour)
        jour_semaine = JOURS_SEMAINE[date_courante.weekday()]
        
        # 20-25 séances par jour réparties sur 4 salles et 7 créneaux
        nb_seances_jour = random.randint(20, 25)
        
        for _ in range(nb_seances_jour):
            film_id, type_film = random.choice(films)
            salle_id = random.randint(1, 4)
            creneau = random.choice(CRENEAUX)
            
            # Capacité de la salle
            capacites = {1: 71, 2: 80, 3: 60, 4: 75}
            capacite = capacites[salle_id]
            
            # Calcul du taux d'occupation basé sur les patterns
            taux_base = patterns_performance[type_film][creneau]
            
            # Bonus week-end
            if jour_semaine in bonus_weekend:
                taux_base *= bonus_weekend[jour_semaine]
            
            # Variation aléatoire réaliste
            taux_final = taux_base + random.gauss(0, 12)  # Écart-type de 12%
            taux_final = max(5, min(100, taux_final))  # Borner entre 5% et 100%
            
            nb_spectateurs = int(capacite * taux_final / 100)
            
            seances.append((
                seance_id, film_id, salle_id, 
                date_courante.strftime('%Y-%m-%d'),
                creneau, jour_semaine, nb_spectateurs, 
                round(taux_final, 2)
            ))
            seance_id += 1
    
    return seances

def generer_spectateurs(conn):
    """Génère les profils détaillés des spectateurs"""
    
    cursor = conn.cursor()
    cursor.execute("SELECT seance_id, nb_spectateurs, creneau FROM seances")
    seances_info = cursor.fetchall()
    
    spectateurs = []
    spectateur_id = 1
    
    # Patterns d'âge par créneau (réalistes selon les tarifs cinéma)
    patterns_age = {
        '09h00': {'Moins de 14': 5, 'Moins de 26': 10, 'Tarif normal (26-60)': 35, 'Senior (60+)': 50},   # Matinée seniors
        '11h30': {'Moins de 14': 10, 'Moins de 26': 15, 'Tarif normal (26-60)': 40, 'Senior (60+)': 35},  # Fin matinée
        '14h00': {'Moins de 14': 15, 'Moins de 26': 20, 'Tarif normal (26-60)': 35, 'Senior (60+)': 30},  # Début après-midi
        '16h30': {'Moins de 14': 20, 'Moins de 26': 25, 'Tarif normal (26-60)': 35, 'Senior (60+)': 20},  # Après-midi
        '18h30': {'Moins de 14': 10, 'Moins de 26': 30, 'Tarif normal (26-60)': 45, 'Senior (60+)': 15},  # Pré-soirée
        '20h30': {'Moins de 14': 5, 'Moins de 26': 35, 'Tarif normal (26-60)': 45, 'Senior (60+)': 15},   # Soirée
        '22h00': {'Moins de 14': 2, 'Moins de 26': 40, 'Tarif normal (26-60)': 40, 'Senior (60+)': 18}    # Fin soirée
    }
    
    for seance_id, nb_spectateurs, creneau in seances_info:
        if nb_spectateurs > 0:
            # Générer les spectateurs de cette séance
            for _ in range(nb_spectateurs):
                # Choisir l'âge selon les patterns du créneau
                weights = list(patterns_age[creneau].values())
                tranche_age = random.choices(TRANCHES_AGE, weights=weights)[0]
                
                spectateurs.append((spectateur_id, seance_id, tranche_age))
                spectateur_id += 1
    
    return spectateurs

def inserer_donnees(conn):
    """Insère toutes les données dans la base"""
    cursor = conn.cursor()
    
    print(" Insertion des salles...")
    cursor.executemany("INSERT INTO salles VALUES (?, ?, ?)", SALLES_CONFIG)
    
    print(" Génération et insertion des films...")
    films = generer_films()
    cursor.executemany("INSERT INTO films VALUES (?, ?, ?, ?, ?)", films)
    print(f"   → {len(films)} films créés")
    
    print(" Génération et insertion des séances...")
    seances = generer_seances(conn)
    cursor.executemany(
        "INSERT INTO seances VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
        seances
    )
    print(f"   → {len(seances)} séances créées")
    
    print(" Génération et insertion des spectateurs...")
    spectateurs = generer_spectateurs(conn)
    cursor.executemany("INSERT INTO spectateurs VALUES (?, ?, ?)", spectateurs)
    print(f"   → {len(spectateurs)} spectateurs créés")
    
    conn.commit()

def exporter_csv(conn):
    """Exporte les données des tables principales en CSV"""
    
    # Export des tables principales uniquement
    print("📄 Export des tables principales...")
    
    # Table films
    df_films = pd.read_sql_query("SELECT * FROM films", conn)
    
    # Table salles
    df_salles = pd.read_sql_query("SELECT * FROM salles", conn)
    
    # Table seances
    df_seances = pd.read_sql_query("SELECT * FROM seances", conn)
    
    # Table spectateurs
    df_spectateurs = pd.read_sql_query("SELECT * FROM spectateurs", conn)
    
    # Créer le dossier data s'il n'existe pas
    base_path = os.path.dirname(DB_PATH)
    data_path = os.path.join(base_path, 'data')
    os.makedirs(data_path, exist_ok=True)
    
    # Sauvegarder les CSV avec encodage UTF-8 + BOM dans le dossier data
    df_films.to_csv(os.path.join(data_path, 'films.csv'), index=False, encoding='utf-8-sig')
    df_salles.to_csv(os.path.join(data_path, 'salles.csv'), index=False, encoding='utf-8-sig')
    df_seances.to_csv(os.path.join(data_path, 'seances.csv'), index=False, encoding='utf-8-sig')
    df_spectateurs.to_csv(os.path.join(data_path, 'spectateurs.csv'), index=False, encoding='utf-8-sig')
    
    print(f"\n CSV exportés dans : {data_path}")
    print(" Tables exportées :")
    print("   • data/films.csv")
    print("   • data/salles.csv") 
    print("   • data/seances.csv")
    print("   • data/spectateurs.csv")
    
    # Retourner les stats pour affichage
    return df_films, df_seances

def afficher_statistiques(conn):
    """Affiche des statistiques de contrôle"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print(" STATISTIQUES DE LA BASE DE DONNÉES")
    print("="*60)
    
    # Comptes généraux
    cursor.execute("SELECT COUNT(*) FROM films")
    nb_films = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM seances")
    nb_seances = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM spectateurs")
    nb_spectateurs = cursor.fetchone()[0]
    
    cursor.execute("SELECT AVG(taux_occupation) FROM seances")
    taux_moyen = cursor.fetchone()[0]
    
    print(f" Films dans le catalogue : {nb_films}")
    print(f" Séances programmées : {nb_seances}")
    print(f" Spectateurs total : {nb_spectateurs}")
    print(f" Taux d'occupation moyen : {taux_moyen:.1f}%")
    
    # Top 5 types de films
    cursor.execute("""
        SELECT f.type_film, AVG(s.taux_occupation) as taux_moyen
        FROM films f JOIN seances s ON f.film_id = s.film_id
        GROUP BY f.type_film
        ORDER BY taux_moyen DESC LIMIT 5
    """)
    
    print(f"\n TOP 5 TYPES DE FILMS (taux d'occupation):")
    for i, (type_film, taux) in enumerate(cursor.fetchall(), 1):
        print(f"   {i}. {type_film}: {taux:.1f}%")

def main():
    """Fonction principale"""
    random.seed(42)  # Pour reproductibilité
    
    print("GÉNÉRATION DE LA BASE DE DONNÉES - CINÉMA LE CONCORDE")
    print("="*65)
    
    # Créer la base
    conn = creer_base_donnees()
    print(f"Base de données créée : {DB_PATH}")
    
    # Insérer les données
    inserer_donnees(conn)
    
    # Statistiques
    afficher_statistiques(conn)
    
    # Export CSV des tables uniquement
    df_films, df_seances = exporter_csv(conn)
    
    # Calculer les stats pour affichage (sans export CSV)
    cursor = conn.cursor()  # Définir le cursor ici
    cursor.execute("""
        SELECT f.type_film, AVG(s.taux_occupation) as taux_moyen
        FROM films f JOIN seances s ON f.film_id = s.film_id
        GROUP BY f.type_film
        ORDER BY taux_moyen DESC LIMIT 4
    """)
    top_4_types = cursor.fetchall()
    
    print(f"\n Top 4 types sélectionnés pour l'analyse :")
    for i, (type_film, taux) in enumerate(top_4_types, 1):
        print(f"   {i}. {type_film}: {taux:.1f}%")
    
    conn.close()
    print(f"\nGénération terminée ! Base prête pour l'analyse.")

if __name__ == "__main__":
    main()