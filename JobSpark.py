from pyspark import SparkConf, SparkContext
import sys

def mapper1(line):

    # Equivalente al Mapper 1: Estrae e pulisce i campi
    parts = line.split('\t')
    if len(parts) < 5:
        return None
    
    title = parts[0].strip()
    name = parts[1].strip()
    role = parts[2].strip()
    rating_str = parts[3].strip()
    votes_str = parts[4].strip()

    # Controlli di validità
    if not title or title in ("NaN", "\\N"): return None
    if not name or name in ("NaN", "\\N"): return None
    if not role or role in ("NaN", "\\N"): return None
    if not rating_str or rating_str in ("NaN", "\\N"): return None
    if not votes_str or votes_str in ("NaN", "\\N"): return None

    try:
        num_votes = int(float(votes_str))
    except ValueError:
        return None

    # Filtro film con almeno 500 voti
    if num_votes < 500:
        return None

    if role in ["actor", "actress", "director"]:
        # Chiave: TitoloFilm, Valore: (Ruolo, Nome, Voto)
        return (title, (role, name, float(rating_str)))
    return None

def reducer1(record):
    # Equivalente al Reducer 1: Prodotto Cartesiano per singolo film
    title, participants = record
    directors = []
    actors = []
    rating = 0.0
    
    for role, name, r in participants:
        rating = r 
        if role == "director":
            directors.append(name)
        else:
            actors.append(name)

    # Skip di titoli con troppi collaboratori per non bloccare il reducer
    if len(directors) > 50 or len(actors) > 500:
        return []

    # Prodotto Cartesiano
    pairs = []
    for d in directors:
        for a in actors:
            if d != a:
                pairs.append((f"{d}_{a}", rating))
    return pairs

def main():
    # Controllo Input
    if len(sys.argv) != 3:
        print("Usage: JobSpark.py <input_path> <output_path>")
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # Inizializzazione del Contesto 
    conf = SparkConf().setAppName("JobSpark")
    sc = SparkContext(conf=conf)

    # 1. Lettura Input (Creazione RDD base) 
    lines_rdd = sc.textFile(input_path)


    # ----
    # EQUIVALENTE JOB 1
    # Map e Filter eliminano i record sporchi
    cleaned_rdd = lines_rdd.map(mapper1).filter(lambda x: x is not None)
    
    # Raggruppiamo tutti i partecipanti per TitoloFilm
    grouped_by_title = cleaned_rdd.groupByKey()
    
    # flatMap genera le coppie partendo da un solo film 
    pairs_rdd = grouped_by_title.flatMap(reducer1)


    # ----
    # EQUIVALENTE JOB 2
    # Equivalente al Mapper2
    mapped_pairs = pairs_rdd.mapValues(lambda rating: (rating, 1))

    # Equivalente del Combiner2: Aggregazione dati
    aggregated_pairs = mapped_pairs.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    def filter_and_avg(record):
        pair, (sum_ratings, count) = record
        if count >= 10:
            return (pair, sum_ratings / count)
        return None
    
    # Equivalente Reducer 2: filtra coppie con almeno 10 collaborazioni
    final_averages_rdd = aggregated_pairs.map(filter_and_avg).filter(lambda x: x is not None)


    # ----
    # EQUIVALENTE JOB 3
    # Ordinamento: ascending=False per ordine decrescente 
    sorted_rdd = final_averages_rdd.repartition(1).sortBy(lambda x: x[1], ascending=False)

    # Output finale come stringa e lo salviamo su HDFS
    formatted_output = sorted_rdd.map(lambda x: f"{x[1]}\t{x[0]}")
    formatted_output.saveAsTextFile(output_path)

    sc.stop()

if __name__ == "__main__":
    main()