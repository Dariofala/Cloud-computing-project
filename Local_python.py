import sys
import time
import tracemalloc
from collections import defaultdict

def elaborazione_locale(input_path, output_path):
    # Iniziamo a tracciare la memoria e il tempo
    tracemalloc.start()
    start_time = time.time()
    
    # Dizionario per raggruppare i partecipanti per film
    # Struttura: { 'idFilm': [('director', 'Nome', 8.5), ('actor', 'Nome', 8.5), ...] }
    movies = defaultdict(list)
    
    print("[1/4] Lettura file e filtraggio iniziale...")
    with open(input_path, 'r', encoding='utf-8') as f:
        for riga in f:
            parts = riga.strip().split('\t')
            if len(parts) < 5:
                continue
            
            idFilm = parts[0].strip()
            nomePersona = parts[1].strip()
            ruolo = parts[2].strip()
            votoStr = parts[3].strip()
            numVotiStr = parts[4].strip()
            
            # Controlli NaN
            if "\\N" in parts or "NaN" in parts:
                continue
                
            try:
                numVoti = int(float(numVotiStr))
                voto = float(votoStr)
            except ValueError:
                continue
                
            # Filtro 100000 voti
            if numVoti < 100000:
                continue
                
            if ruolo in ["director", "actor", "actress"]:
                movies[idFilm].append((ruolo, nomePersona, voto))

    print("[2/4] Generazione del prodotto cartesiano...")
    # Struttura: { 'Regista_Attore': [SommaVoti, ConteggioCollaborazioni] }
    pair_stats = defaultdict(lambda: [0.0, 0])
    
    for idFilm, participants in movies.items():
        directors = [p[1] for p in participants if p[0] == "director"]
        actors = [p[1] for p in participants if p[0] in ("actor", "actress")]
        
        if not directors or not actors: continue
        if len(directors) > 50 or len(actors) > 500: continue
        
        voto_film = participants[0][2] # Il voto è uguale per tutti in quel film
        
        for d in directors:
            for a in actors:
                if d != a:
                    pair_key = f"{d}_{a}"
                    pair_stats[pair_key][0] += voto_film
                    pair_stats[pair_key][1] += 1

    # Puliamo il dizionario dei film per liberare RAM prima del sort
    movies.clear()

    print("[3/4] Aggregazione, calcolo medie e filtro sbarramento...")
    final_results = []
    for pair, stats in pair_stats.items():
        somma_voti = stats[0]
        conteggio = stats[1]
        
        if conteggio >= 3: # Filtro min. 3 collaborazioni
            media = somma_voti / conteggio
            final_results.append((media, pair))

    print("[4/4] Ordinamento decrescente e scrittura su file...")
    final_results.sort(key=lambda x: x[0], reverse=True)

    with open(output_path, 'w', encoding='utf-8') as out_f:
        for media, pair in final_results:
            # Formattiamo la media per evitare numeri periodici infiniti
            out_f.write(f"{media:.2f}\t{pair}\n")

    # --- CALCOLO METRICHE ---
    end_time = time.time()
    current_mem, peak_mem = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    execution_time = end_time - start_time
    peak_mem_mb = peak_mem / (1024 * 1024)
    
    print("\n" + "="*40)
    print("      METRICHE DI ESECUZIONE LOCALE      ")
    print("="*40)
    print(f"Tempo totale di esecuzione : {execution_time:.2f} secondi")
    print(f"Picco di Memoria RAM usata : {peak_mem_mb:.2f} MB")
    print(f"Coppie valide trovate      : {len(final_results)}")
    print("="*40)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso corretto: python Local_python.py <input_dataset.tsv> <output_results.tsv>")
        sys.exit(-1)
        
    file_input = sys.argv[1]
    file_output = sys.argv[2]
    
    elaborazione_locale(file_input, file_output)