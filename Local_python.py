import sys
import time
from collections import defaultdict


def elaborazione_locale(input_path, output_path):
    # Tempo reale (wall-clock) di inizio
    start_wall = time.perf_counter()

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

            if "\\N" in parts or "NaN" in parts:
                continue

            try:
                numVoti = int(float(numVotiStr))
                voto = float(votoStr)
            except ValueError:
                continue

            if numVoti < 100000:
                continue

            if ruolo in ["director", "actor", "actress"]:
                movies[idFilm].append((ruolo, nomePersona, voto))


    print("[2/4] Generazione del prodotto cartesiano...")
    pair_stats = defaultdict(lambda: [0.0, 0])

    for idFilm, participants in movies.items():
        directors = [p[1] for p in participants if p[0] == "director"]
        actors = [p[1] for p in participants if p[0] in ("actor", "actress")]

        if not directors or not actors: continue
        if len(directors) > 50 or len(actors) > 500: continue

        voto_film = participants[0][2]

        for d in directors:
            for a in actors:
                if d != a:
                    pair_key = f"{d}_{a}"
                    pair_stats[pair_key][0] += voto_film
                    pair_stats[pair_key][1] += 1

    movies.clear()

    print("[3/4] Aggregazione, calcolo medie e filtro sbarramento...")
    final_results = []
    for pair, stats in pair_stats.items():
        somma_voti = stats[0]
        conteggio = stats[1]
        if conteggio >= 3:
            media = somma_voti / conteggio
            final_results.append((media, pair))

    print("[4/4] Ordinamento decrescente e scrittura su file...")
    final_results.sort(key=lambda x: x[0], reverse=True)

    with open(output_path, 'w', encoding='utf-8') as out_f:
        for media, pair in final_results:
            out_f.write(f"{media:.2f}\t{pair}\n")

    # --- CALCOLO METRICHE ---
    end_wall = time.perf_counter()

    execution_time = end_wall - start_wall          # tempo reale (s)

    print("\n" + "="*40)
    print("      METRICHE DI ESECUZIONE LOCALE      ")
    print("="*40)
    print(f"Real execution time : {execution_time:.2f} s")
    print(f"Coppie valide trovate : {len(final_results)}")
    print("="*40)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso corretto: python Local_python.py <input_dataset.tsv> <output_results.tsv>")
        sys.exit(-1)

    elaborazione_locale(sys.argv[1], sys.argv[2])