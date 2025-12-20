import json
from pathlib import Path

path = Path("BuyukVeri_Pipeline_Projesi_v2.ipynb")
nb = json.loads(path.read_text(encoding='utf-8'))

feature_list = [
    'Start_Lat', 'Start_Lng', 'Distance(mi)', 'Temperature(F)',
    'Humidity(%)', 'Pressure(in)', 'Visibility(mi)', 'Wind_Speed(mph)',
    'Precipitation(in)', 'Weather_Condition_Index', 'Sunrise_Sunset_Index',
    'Civil_Twilight_Index', 'State_Index'
]

rf_code = """# Random Forest (PySpark) - TUM veri (~7.7M kayit)
print("Random Forest Egitimi (PySpark, tam veri)")
print("="*60)

# Ozellik listesi (kNN ile ayni). Daha once tanimlanmadiysa burada kur.
if 'feature_columns' not in globals():
    feature_columns = FEATURE_LIST
    # kNN oncesindeki temizlikle tutarlilik icin eksik degerleri temizle
    for col_name in feature_columns:
        df_sample = df_sample.filter(col(col_name).isNotNull())

rf_feature_cols = feature_columns

# Severity'yi tip olarak guvenceye al
df_rf_base = (
    df_sample.select(rf_feature_cols + ['Severity'])
    .withColumn('Severity', col('Severity').cast(IntegerType()))
)

# Ozellik vektoru
rf_assembler = VectorAssembler(inputCols=rf_feature_cols, outputCol="rf_features", handleInvalid="skip")
df_rf = rf_assembler.transform(df_rf_base).select('rf_features', 'Severity').cache()

rf_total = df_rf.count()
print(f"Toplam kayit (RF): {rf_total:,}")

# Egitim/Test bolme (once cache, ekstra count yok)
train_rf, test_rf = df_rf.randomSplit([0.8, 0.2], seed=42)
train_rf = train_rf.cache()
test_rf = test_rf.cache()
train_rows_est = int(rf_total * 0.8)
test_rows_est = rf_total - train_rows_est
print(f"   - Egitim/Test bolme tamamlandi ve cache'lendi (yaklasik {train_rows_est:,}/{test_rows_est:,})")

# Model (maxBins yuksek tutuldu; en yuksek kategorik feature ~146 deger)
rf_clf = RandomForestClassifier(
    labelCol='Severity',
    featuresCol='rf_features',
    numTrees=120,
    maxDepth=15,
    maxBins=512,
    subsamplingRate=0.8,
    seed=42,
    featureSubsetStrategy='auto'
)

rf_model = rf_clf.fit(train_rf)
rf_predictions = rf_model.transform(test_rf)

# Metrikler
rf_eval = MulticlassClassificationEvaluator(labelCol='Severity', predictionCol='prediction')
rf_accuracy = rf_eval.evaluate(rf_predictions, {rf_eval.metricName: "accuracy"})
rf_f1 = rf_eval.evaluate(rf_predictions, {rf_eval.metricName: "f1"})
rf_precision = rf_eval.evaluate(rf_predictions, {rf_eval.metricName: "weightedPrecision"})
rf_recall = rf_eval.evaluate(rf_predictions, {rf_eval.metricName: "weightedRecall"})

print(f"RF Accuracy: {rf_accuracy:.4f}")
print(f"RF F1-Score: {rf_f1:.4f}")
print(f"RF Precision (Weighted): {rf_precision:.4f}")
print(f"RF Recall (Weighted): {rf_recall:.4f}")

# Ozellik onemleri (ilk 10)
rf_importances = list(zip(rf_feature_cols, rf_model.featureImportances.toArray()))
rf_top_features = sorted(rf_importances, key=lambda x: x[1], reverse=True)[:10]

print("\nEn onemli 10 ozellik:")
for feat, score in rf_top_features:
    print(f"   - {feat}: {score:.4f}")

rf_metrics = {
    'model': 'RandomForestClassifier',
    'num_trees': int(rf_clf.getOrDefault('numTrees')),
    'max_depth': int(rf_clf.getOrDefault('maxDepth')),
    'max_bins': int(rf_clf.getOrDefault('maxBins')),
    'train_rows': int(train_rows_est),
    'test_rows': int(test_rows_est),
    'accuracy': float(rf_accuracy),
    'f1_weighted': float(rf_f1),
    'precision_weighted': float(rf_precision),
    'recall_weighted': float(rf_recall),
    'top_feature_importances': [
        {'feature': f, 'importance': float(s)} for f, s in rf_top_features
    ]
}
"""

rf_code = rf_code.replace('FEATURE_LIST', repr(feature_list))
nb['cells'][24]['source'] = rf_code.splitlines(True)

feat_code = """# Ozellik vektoru olustur
if 'feature_columns' not in globals():
    feature_columns = FEATURE_LIST

# Eksik degerleri tekrar kontrol et ve temizle
for col_name in feature_columns:
    df_sample = df_sample.filter(col(col_name).isNotNull())

print(f"Temiz veri sayisi: {df_sample.count():,}")
"""
feat_code = feat_code.replace('FEATURE_LIST', repr(feature_list))
nb['cells'][26]['source'] = feat_code.splitlines(True)

path.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding='utf-8')
print('Notebook updated')
