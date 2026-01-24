%% Real-Time E-commerce Streaming Platform (Local-first)
%% Mermaid: https://mermaid.js.org/

flowchart LR

  %%  Source + Bus
  subgraph SRC[Event Source]
    GEN["Generator\nservices/generator\nJSON events"]
  end

  subgraph BUS[Streaming Bus]
    RP["Redpanda\nKafka broker"]
    TOPIC[("Kafka topic\n ecom.events.raw.v1")]
    CONS["Redpanda Console\nhttp://localhost:8080"]
    RP --> TOPIC
    CONS --- RP
  end

  GEN -->|produce| TOPIC

  %%  Lake + Storage 
  subgraph STG[Storage]
    MIO["MinIO (S3)\nhttp://localhost:9001\nbucket: lake"]
    BRZ["Bronze parquet\n/lake/bronze/ecom_events/v1/\npartition: ingest_date/hour"]
    SIL["Silver parquet\n/lake/silver/ecom_events/v1/\npartition: event_date/hour"]
    QRT["Quarantine parquet\n/lake/quarantine/ecom_events/v1/\ncontains reason + raw payload"]
    GLD["Gold aggregates\n/lake/gold/kpi_minute/v1/\npartition: event_date/hour"]
  end

  %%  Compute 
  subgraph CMP[Compute Spark Structured Streaming']
    BW["Bronze Writer\nservices/bronze-writer\nKafka → Bronze"]
    SW["Silver Writer\nservices/silver-writer\nBronze → (Silver + Quarantine)"]
    GW["Gold Writer\nservices/gold-writer\nSilver → Gold KPIs"]
  end

  TOPIC -->|read| BW
  BW -->|write| BRZ
  BRZ -->|read| SW
  SW -->|write valid| SIL
  SW -->|write invalid| QRT
  SIL -->|read| GW
  GW -->|write| GLD

  MIO --- BRZ
  MIO --- SIL
  MIO --- QRT
  MIO --- GLD

  %%  Serving 
  subgraph SV[Serving + Apps]
    PG["Postgres\nschema: serving\nkpi_minute, alerts"]
    API["FastAPI\n/services/api\nhttp://localhost:8000\nOpenAPI /docs"]
    UI["Streamlit Dashboard\n/services/dashboard\nhttp://localhost:8501"]
    DET["Anomaly Detector\n/services/anomaly-detector\npolls KPIs → writes alerts"]
  end

  GLD -->|batch/stream load| PG
  DET -->|read KPIs| PG
  DET -->|write alerts| PG
  API -->|read| PG
  UI -->|calls API| API

  %%  Notes 
  %% Bronze: raw-ish, minimal typing, append-only
  %% Silver: typed + validated + deduped; quarantine invalid rows
  %% Gold: business-ready aggregates for serving layer
