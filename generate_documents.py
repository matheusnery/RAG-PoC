import json
import random
import datetime
import os

random.seed(42)
start = datetime.date(2025, 10, 1)
days = 180
teams = [
    {
        'name': 'Sales Pipeline ETL',
        'owners': ['Sarah Chen'],
        'base_volume': 450,
        'frequency': 'daily at 02:00 UTC',
        'data_points': ['transaction data', 'CRM leads', 'payment gateway logs', 'inventory updates']
    },
    {
        'name': 'Customer Analytics',
        'owners': ['Marcus Johnson', 'Priya Patel'],
        'base_volume': 180,
        'frequency': 'every 6 hours',
        'data_points': ['web tracking', 'mobile app sessions', 'call center interactions']
    },
    {
        'name': 'Supply Chain & Logistics',
        'owners': ['Thomas Rodriguez'],
        'base_volume': 650,
        'frequency': 'every 4 hours',
        'data_points': ['shipment tracking', 'warehouse inventory', 'supplier ETL feeds']
    },
    {
        'name': 'Financial Reporting',
        'owners': ['Jennifer Walsh'],
        'base_volume': 320,
        'frequency': 'hourly',
        'data_points': ['GL entries', 'bank reconciliations', 'currency conversions']
    },
    {
        'name': 'Marketing Campaign Metrics',
        'owners': ['Alex Kim', 'Rebecca Stone'],
        'base_volume': 1200,
        'frequency': 'daily full refresh plus event ingestion',
        'data_points': ['email clicks', 'paid search', 'social impressions']
    },
    {
        'name': 'HR & Talent Analytics',
        'owners': ['Patricia Moore', 'David Lee'],
        'base_volume': 0.85,
        'frequency': 'weekly on Monday',
        'data_points': ['employee records', 'performance ratings', 'training completions']
    }
]

error_patterns = [
    'Traceback (most recent call last):\n  File "/app/etl/sales_ingest.py", line 142, in <module>\n    process_sales_batch(batch)\n  File "/app/etl/sales_ingest.py", line 98, in process_sales_batch\n    amount = int(row["amount"])\nValueError: invalid literal for int() with base 10: \'N/A\'',
    'pyspark.sql.utils.AnalysisException: Path does not exist: \'s3://data-bucket/supply_chain/2026/04/12/*\'\n  at org.apache.spark.sql.execution.datasources.DataSource.readSchema(DataSource.scala:215)\n  at org.apache.spark.sql.execution.datasources.DataSource.resolveRelation(DataSource.scala:380)\n  at org.apache.spark.sql.execution.datasources.DataSource.getTableMetadata(DataSource.scala:117)',
    'pyarrow.lib.ArrowInvalid: Could not convert 1.23.45 to int64 in column customer_id, while converting column source: CSV',
    'AssertionError: daily quality checks failed, duplicate keys found in customer_id dimension, total duplicates=4, expected 0',
    'RuntimeError: Data validation rule \"country_code_format\" failed for 112 records, sample values: [\'usa\', \'BR\', \'N\']',
    'pyspark.sql.utils.IllegalArgumentException: requirement failed: Unsupported file format: \'xml\' for path /mnt/data/marketing/feeds/2026-04-10',
    'TypeError: cannot concatenate \'NoneType\' and \'str\' objects while parsing employee record line 3348',
    'OperationalError: database is locked: failed to write audit event to metadata catalog',
]

entries = []
entry_id = 1
for day in range(days):
    current_date = start + datetime.timedelta(days=day)
    date_str = current_date.isoformat()
    weekday = current_date.weekday()
    for team in teams:
        base = team['base_volume']
        if team['name'] == 'HR & Talent Analytics':
            volume = round(base * 1000, 1) if weekday == 0 else round(base * 1000 * 0.15, 1)
        else:
            variation = random.uniform(-0.18, 0.18)
            volume = round(base * (1 + variation), 2)
        quality_base = 96.0 if team['name'] == 'Marketing Campaign Metrics' else 98.0
        quality = round(min(100.0, quality_base + random.uniform(-4.0, 1.0)), 2)
        status = 'completed successfully'
        quality_issue = ''
        failure_description = ''
        data_notes = []
        if team['name'] == 'HR & Talent Analytics' and weekday != 0:
            status = 'skipped due to schedule'
            run_time = 'N/A'
            quality = round(100.0 - random.uniform(0.0, 0.2), 2)
        else:
            run_hours = random.randint(1, 5)
            run_minutes = random.randint(0, 59)
            run_time = f"{run_hours}h {run_minutes}m"
            if random.random() < 0.18:
                status = random.choice(['failed during processing', 'completed with warnings'])
                error = random.choice(error_patterns)
                failure_description = error
                if 'duplicate' in error or 'ArrowInvalid' in error or 'Data validation' in error:
                    quality -= random.uniform(2.5, 6.5)
                    quality_issue = 'data quality checks failed'
                else:
                    quality -= random.uniform(1.0, 4.0)
                    quality_issue = 'pipeline processing error'
                if status == 'failed during processing':
                    if 'pyspark.sql.utils.AnalysisException' in error:
                        data_notes.append('Spark job failed while reading S3 path.')
                    elif 'Traceback' in error:
                        data_notes.append('Python ETL crashed during transform stage.')
                    elif 'TypeError' in error:
                        data_notes.append('Schema mismatch in employee import feed.')
                    else:
                        data_notes.append('Data ingest stage encountered a runtime exception.')
            else:
                if random.random() < 0.2:
                    status = 'completed with warnings'
                    warn = random.choice([
                        '2% of records flagged for manual review',
                        'partial dimension refresh due to late arrivals',
                        'quality check threshold borderline but accepted',
                    ])
                    data_notes.append(warn)
                if random.random() < 0.12:
                    quality -= random.uniform(0.4, 1.8)
                    quality_issue = 'slight data quality degradation detected'
        quality = round(max(85.0, min(100.0, quality)), 2)
        if quality < 95.0 and not quality_issue:
            quality_issue = 'data quality degradation detected'
        text = [
            f"Report date: {date_str}",
            f"Team: {team['name']}",
            f"Owners: {', '.join(team['owners'])}",
            f"Execution frequency: {team['frequency']}",
            f"Daily raw data volume processed: {volume}GB",
            f"Primary data sources: {', '.join(team['data_points'])}",
            f"Pipeline status: {status}",
            f"Run duration: {run_time}",
            f"Data quality score: {quality}%",
        ]
        if quality_issue:
            text.append(f"Data quality note: {quality_issue}")
        if data_notes:
            text.append('Notes: ' + ' '.join(data_notes))
        if failure_description:
            text.append('Error details: ' + failure_description)
        if team['name'] == 'Marketing Campaign Metrics' and status == 'completed successfully' and random.random() < 0.14:
            text.append('Attribute harmonization warning: inconsistent channel naming detected in campaign source feed.')
        if team['name'] == 'Supply Chain & Logistics' and random.random() < 0.1:
            text.append('External validation result: 5 shipment records failed carrier API reconciliation.')
        if team['name'] == 'Financial Reporting' and random.random() < 0.08:
            text.append('Audit trail: 1 reconciliation mismatch detected between region EMEA and APAC.')
        doc = {
            'id': entry_id,
            'title': f"{team['name']} Daily Report {date_str}",
            'team': team['name'],
            'date': date_str,
            'status': status,
            'quality_score': quality,
            'text': ' '.join(text)
        }
        entries.append(doc)
        entry_id += 1
extra_scenarios = [
    {
        'title': 'Pipeline Retry Storm - late arrivals and downstream dependency failure',
        'team': 'Sales Pipeline ETL',
        'snippet': 'A downstream dependency on Inventory API failed, causing three consecutive retries. Triggered dead-letter ingest recovery and manual intervention.',
        'error': 'RuntimeError: downstream service timeout after 900 seconds while polling Inventory API',
    },
    {
        'title': 'Malformed CSV spike in marketing clickstream',
        'team': 'Marketing Campaign Metrics',
        'snippet': 'Inbound vendor feed delivered malformed CSV with mismatched header count. Auto-parse fallback flagged 12,000 rows as invalid.',
        'error': 'pyspark.sql.utils.ArithmeticException: overflow in expression',
    },
    {
        'title': 'Late payroll adjustments with HR record duplicates',
        'team': 'HR & Talent Analytics',
        'snippet': 'HRIS payroll load encountered duplicate employee records and manual merge steps were required.',
        'error': 'ValueError: Duplicate key \"employee_id\" found during dimensional load',
    },
    {
        'title': 'Financial currency conversion mismatch',
        'team': 'Financial Reporting',
        'snippet': 'Daily currency conversion pipeline reported mismatch between API rate and accounting source, delaying close process.',
        'error': 'AssertionError: Foreign exchange rate deviation exceeds allowed threshold of 0.35%',
    }
]
for scenario in extra_scenarios:
    entries.append({
        'id': entry_id,
        'title': scenario['title'],
        'team': scenario['team'],
        'date': '2026-04-15',
        'status': 'failed during processing',
        'quality_score': round(random.uniform(80.0, 94.5), 2),
        'text': f"Report date: 2026-04-15 Team: {scenario['team']} Owners: multiple. Execution frequency: daily. Pipeline status: failed during processing. Data quality score: {round(random.uniform(80.0, 94.5),2)}%. {scenario['snippet']} Error details: {scenario['error']}"
    })
    entry_id += 1
path = os.path.join(os.getcwd(), 'documents.json')
with open(path, 'w', encoding='utf-8') as f:
    json.dump(entries, f, indent=2, ensure_ascii=False)
print(f'Wrote {len(entries)} documents to {path}')
