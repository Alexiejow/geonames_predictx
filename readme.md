## How to Run

### 1. Clone the Repository:

```bash
git clone https://github.com/yourusername/geonames_predictx.git
cd geonames_predictx
```

### 2. Set Up the Virtual Environment:

```bash
python3 -m venv venv
source venv/bin/activate    # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Run the Pipeline:

To download the dataset, filter locations, assign metropolies, and generate visualizations, run:

```bash
python scripts/run_pipeline.py
python scripts/visualise_points_polygons.py
```

These scripts will download data into `data/raw/`, perform data cleaning, metropolis assignment, and generate checkpoint and output CSV files into `data/processed/`, and interactive HTML maps in `data/visualisations`.

---

## Exploratory Data Analysis Summary

The EDA (see /notebooks folder) includes:

- **Data Loading:** Importing raw GeoNames data and setting appropriate headers.
- **Population Filtering:** Distinguishing between locations with zero and non-zero populations, and analyzing various feature classes (e.g., PPL, PPLA, PPLC).
- **Location Categories:** Identifying and analyzing different categories of populated places and administrative units, including duplicate names and overlaps.
- **Visualization:** Initial maps displaying locations in Poland, with markers color-coded based on feature codes.

### Filtering of nested locations
During EDA, it was observed that many smaller locations (e.g., districts or subdivisions) are nested within the borders of larger administrative areas. To improve clarity, a feature was implemented to remove locations that are already included in the borders of larger cities.

This step helps reduce visual clutter and ensures the final visualization displays only distinct, non-overlapping locations.

---

## Future Enhancements

- **Iterative Metropolis Assignment:** Implement an iterative algorithm where metropolises "grow" by annexing nearby towns, updating effective populations, and reassigning until convergence.
- **Map Visualizations per Iteration:** Generate a series of interactive maps to visualize each iteration of the assignment process.
- **PySpark Integration:** Transition parts of the pipeline to PySpark for improved scalability and efficiency. Performance comparisons between Python and Spark implementations will be explored.
- **Other Country Data:** Test and adapt the pipeline to other countries, as well as processing all Geonames data in sequence.

---

## Dependencies

- Python 3.x
- pandas
- folium
- numpy
- scikit-learn
- ...

(Other dependencies are listed in `requirements.txt`)

---

## License

This project is licensed under the MIT License.

