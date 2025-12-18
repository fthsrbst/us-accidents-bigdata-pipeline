#!/usr/bin/env python3
"""
Pipeline Orchestrator
Tüm pipeline adımlarını sırayla çalıştırır
"""

import os
import sys
import time
import subprocess
from datetime import datetime

# Script dizini
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Pipeline adımları
PIPELINE_STEPS = [
    {
        'name': 'Veri İndirme',
        'script': 'download_data.py',
        'description': 'Kaggle\'dan US Accidents veri setini indir',
        'required': True
    },
    {
        'name': 'Kafka Producer',
        'script': 'kafka_producer.py',
        'description': 'CSV verisini Kafka\'ya stream et',
        'required': False  # Opsiyonel - batch yükleme de yapılabilir
    },
    {
        'name': 'Spark Streaming / HDFS Yükleme',
        'script': 'spark_streaming.py',
        'args': ['--batch'],  # Batch modunda çalıştır
        'description': 'Veriyi HDFS\'e yükle',
        'required': True
    },
    {
        'name': 'Hive Tabloları',
        'script': 'hdfs_to_hive.py',
        'description': 'HDFS verisini Hive tablosu olarak tanımla',
        'required': True
    },
    {
        'name': 'Veri Temizleme',
        'script': 'data_cleaning.py',
        'description': 'Veriyi temizle ve özellik mühendisliği yap',
        'required': True
    },
    {
        'name': 'kNN Sınıflandırma',
        'script': 'knn_classification.py',
        'description': 'kNN modeli eğit ve değerlendir',
        'required': True
    },
    {
        'name': 'K-Means Kümeleme',
        'script': 'kmeans_clustering.py',
        'description': 'K-Means kümeleme analizi yap',
        'required': True
    },
    {
        'name': 'MongoDB Export',
        'script': 'mongodb_export.py',
        'description': 'Sonuçları MongoDB\'ye yaz',
        'required': True
    }
]

def print_header():
    """Header yazdır"""
    print("=" * 70)
    print("                 BIG DATA PIPELINE ORCHESTRATOR")
    print("         HDFS + Hive + Kafka + Spark + MongoDB")
    print("=" * 70)
    print(f"Başlangıç Zamanı: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

def print_step(step_num, total_steps, step_info):
    """Adım bilgisi yazdır"""
    print("\n" + "-" * 70)
    print(f"ADIM {step_num}/{total_steps}: {step_info['name']}")
    print(f"Açıklama: {step_info['description']}")
    print(f"Script: {step_info['script']}")
    print("-" * 70)

def run_script(script_name, args=None):
    """Bir Python script'i çalıştır"""
    script_path = os.path.join(SCRIPT_DIR, script_name)
    
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script bulunamadı: {script_path}")
    
    cmd = [sys.executable, script_path]
    if args:
        cmd.extend(args)
    
    start_time = time.time()
    
    result = subprocess.run(
        cmd,
        cwd=SCRIPT_DIR,
        capture_output=False,  # Çıktıyı göster
        text=True
    )
    
    elapsed_time = time.time() - start_time
    
    return result.returncode == 0, elapsed_time

def run_pipeline(skip_steps=None, only_steps=None):
    """Pipeline'ı çalıştır"""
    print_header()
    
    skip_steps = skip_steps or []
    results = []
    total_time = 0
    
    # Filtrele
    if only_steps:
        steps_to_run = [s for s in PIPELINE_STEPS if s['name'] in only_steps]
    else:
        steps_to_run = [s for s in PIPELINE_STEPS if s['name'] not in skip_steps]
    
    print(f"\nÇalıştırılacak adım sayısı: {len(steps_to_run)}")
    
    for i, step in enumerate(steps_to_run, 1):
        print_step(i, len(steps_to_run), step)
        
        try:
            success, elapsed = run_script(
                step['script'], 
                step.get('args', None)
            )
            
            total_time += elapsed
            
            if success:
                results.append({
                    'step': step['name'],
                    'status': 'SUCCESS',
                    'time': elapsed
                })
                print(f"\n✓ {step['name']} tamamlandı ({elapsed:.2f} saniye)")
            else:
                results.append({
                    'step': step['name'],
                    'status': 'FAILED',
                    'time': elapsed
                })
                print(f"\n✗ {step['name']} başarısız!")
                
                if step.get('required', False):
                    print("Bu adım zorunlu, pipeline durduruluyor...")
                    break
                    
        except Exception as e:
            print(f"\n✗ HATA: {str(e)}")
            results.append({
                'step': step['name'],
                'status': 'ERROR',
                'error': str(e)
            })
            
            if step.get('required', False):
                break
    
    # Özet
    print("\n" + "=" * 70)
    print("                         PIPELINE ÖZET")
    print("=" * 70)
    
    success_count = sum(1 for r in results if r['status'] == 'SUCCESS')
    failed_count = sum(1 for r in results if r['status'] != 'SUCCESS')
    
    print(f"\nToplam süre: {total_time:.2f} saniye ({total_time/60:.2f} dakika)")
    print(f"Başarılı: {success_count}/{len(results)}")
    print(f"Başarısız: {failed_count}/{len(results)}")
    
    print("\nAdım Detayları:")
    for r in results:
        status_icon = "✓" if r['status'] == 'SUCCESS' else "✗"
        time_str = f"{r.get('time', 0):.2f}s" if 'time' in r else "N/A"
        print(f"  {status_icon} {r['step']}: {r['status']} ({time_str})")
    
    print("\n" + "=" * 70)
    
    if failed_count == 0:
        print("✓ PIPELINE BAŞARIYLA TAMAMLANDI!")
    else:
        print("✗ PIPELINE HATALARLA TAMAMLANDI!")
    
    print("=" * 70)
    
    return results

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Big Data Pipeline Orchestrator')
    parser.add_argument('--skip', nargs='+', help='Atlanacak adımlar')
    parser.add_argument('--only', nargs='+', help='Sadece çalıştırılacak adımlar')
    parser.add_argument('--list', action='store_true', help='Adımları listele')
    
    args = parser.parse_args()
    
    if args.list:
        print("\nPipeline Adımları:")
        for i, step in enumerate(PIPELINE_STEPS, 1):
            req = "(Zorunlu)" if step.get('required') else "(Opsiyonel)"
            print(f"  {i}. {step['name']} {req}")
            print(f"     Script: {step['script']}")
            print(f"     {step['description']}")
        return
    
    run_pipeline(skip_steps=args.skip, only_steps=args.only)

if __name__ == "__main__":
    main()
