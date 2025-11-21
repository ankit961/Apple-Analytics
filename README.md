here’s a crisp set of **copy-pasteable flow charts** for everything we planned for the analytics dashboard—data in, curation, querying, and the business views.

---

## 1) System overview (context)

```mermaid
flowchart LR
  A["Apple App Store Connect<br/>(Analytics + Sales APIs)"] -->|CSV/TSV.GZ| B["Raw S3<br/>appstore/raw + raw_sales"]
  B --> C["Curators<br/>normalize -> cast -> aggregate"]
  C -->|Parquet| D["Curated S3<br/>appstore/curated/*/dt=.../app_id=.../"]
  D --> E["Athena/Glue<br/>(Partition Projection)"]
  E --> F["Dashboards<br/>(QuickSight / Superset / Streamlit)"]
  F --> G["Business Users"]

```

---

## 2) ONE_TIME backfill (historical) – request → poll → land

```mermaid
flowchart TD
  A["Start Backfill<br/>(app_id list, date range)"] --> B["Chunk ranges <= 90 days"]
  B --> C["Build Request Payload<br/>accessType=ONE_TIME_SNAPSHOT<br/>granularity=DAILY<br/>reports=[type]"]
  C --> D["Submit analyticsReportRequests"]
  D --> E{HTTP 2xx?}
  E -- "no" --> E1["Log error JSON<br/>fix reportType/attributes"] --> C
  E -- "yes" --> F["Poll request.instances.segments.files"]
  F -->|"Download files"| G["Raw S3<br/>appstore/raw/analytics/<type>/dt=.../app_id=.../"]
  G --> H{"New files in last 24h?"}
  H -- "yes" --> F
  H -- "no" --> I["Mark request window COMPLETE"]
```

---

## 3) ONGOING daily feed – rolling updates

```mermaid
flowchart TD
  A["Create ONGOING request<br/>(granularity=DAILY)"] --> B["Daily Poller (cron)"]
  B --> C["Check today's instance/segment files"]
  C -- "available" --> D["Raw S3 ingest (by dt/app_id)"]
  C -- "not yet" --> B
  D --> E["Curate -> Parquet -> Curated S3"]
  E --> F["Dashboards refresh"]
```

---

## 4) Sales & Trends (purchases/units) pipeline

```mermaid
flowchart LR
  A["Sales Reports API<br/>(salesReports)"] -->|filters: reportType, frequency, reportDate, vendorNumber| B["Downloader"]
  B --> C["Raw S3<br/>appstore/raw_sales/SALES/freq/reportDate=YYYY-MM-DD"]
  C --> D["Purchases Curator<br/>parse TSV -> normalize -> cast -> aggregate"]
  D -->|Parquet| E["Curated S3<br/>appstore/curated/purchases/dt=report_date/"]
  E --> F["Athena<br/>(curated.purchases)"]

```

---

## 5) Curation flow (common pattern, all analytics tables)

```mermaid
flowchart TD
  A["Raw CSV/TSV (.gz)"] --> B["Header Normalize<br/>(semantic renames)"]
  B --> C["Type Casting<br/>(Int64, Double, Date)"]
  C --> D["Fill Missing & Keys<br/>(app_id, metric_date, country, ...)"]
  D --> E["Aggregate / Dedupe<br/>(sum by keys)"]
  E --> F["Write Parquet<br/>curated/<table>/dt=.../app_id=.../part-*.parquet"]

```

---

## 6) Athena readiness (projection + query hygiene)

```mermaid
flowchart LR
  A["Curated S3<br/>(Hive-style partitions)"] --> B["DDL with Partition Projection<br/>dt format/range, app_id_part integer,<br/>storage.location.template matches S3"]
  B --> C["Queries<br/>ALWAYS filter dt AND app_id_part,<br/>avoid SELECT *"]
  C --> D["Fast planning, low scan bytes,<br/>stable dashboards"]

```

---

## 7) Dashboard UX flows (role-based)

### 7a) Executive Overview (5-second health)

```mermaid
flowchart TD
  A["Global Filters<br/>(Date, App, Platform, Country)"] --> B["KPI Tiles<br/>Impressions, PPV, FTD, TD, Revenue,<br/>CVRs IMP->PPV, PPV->FTD, IMP->FTD"]
  B --> C["Trend Sparklines"]
  B --> D["Delta vs previous period"]

```

### 7b) Acquisition & ASO

```mermaid
flowchart LR
  A["Engagement Table"] --> B["Daily Funnel Chart<br/>Impressions -> PPV -> FTD -> TD"]
  A --> C["Source x Page x Platform (bars)"]
  A --> D["Top Countries (heatmap)"]
  A --> E["Search Terms Leaderboard<br/>(Impressions, PPV, Term CVR)"]

```

### 7c) Monetization

```mermaid
flowchart LR
  P["Purchases Table"] --> Q["Revenue Trend"]
  P --> R["Units / Revenue by SKU"]
  P --> S["Revenue per Download<br/>(join downloads)"]
  P --> T["Country & Currency Mix"]

```

### 7d) Quality & Brand (Reviews)

```mermaid
flowchart LR
  A["Reviews Table"] --> B["Rating Trend"]
  A --> C["Topic Clusters + Sentiment"]
  C --> D["Impact Overlay<br/>(CVR dips vs negative themes)"]

```

### 7e) Ops & Data Health

```mermaid
flowchart TD
  A["Freshness Monitors<br/>(last dt per table)"] --> B["Alerts<br/>(no new files, poller failures)"]
  B --> C["Runbooks Links"]
  A --> D["Missing Partitions Radar"]

```

---

## 8) Drilldown navigation (from KPIs to root-cause)

```mermaid
flowchart LR
  KPI["Tile spike/dip"] --> A["Which country/platform?"]
  A --> B["Which source/page type?"]
  B --> C["Which search terms?"]
  C --> D["Which SKU / revenue segment?"]
  D --> KPI

```

---

## 9) Data model map (high level)

```mermaid
classDiagram
  class engagement {
    +app_id: BIGINT
    +metric_date: DATE
    +impressions: BIGINT
    +impressions_unique: BIGINT
    +product_page_views: BIGINT
    +product_page_views_unique: BIGINT
    +source_type: STRING
    +page_type: STRING
    +platform: STRING
    +country: STRING
    <<partition>> dt: STRING, app_id_part: BIGINT
  }
  class downloads {
    +app_id: BIGINT
    +metric_date: DATE
    +first_time_downloads: BIGINT
    +redownloads: BIGINT
    +total_downloads: BIGINT
    +platform: STRING
    +country: STRING
    <<partition>> dt: STRING, app_id_part: BIGINT
  }
  class search_terms {
    +app_id: BIGINT
    +metric_date: DATE
    +search_term: STRING
    +impressions: BIGINT
    +product_page_views: BIGINT
    <<partition>> dt: STRING, app_id_part: BIGINT
  }
  class deletions {
    +app_id: BIGINT
    +metric_date: DATE
    +deletions: BIGINT
    +country: STRING
    <<partition>> dt: STRING, app_id_part: BIGINT
  }
  class purchases {
    +app_id: BIGINT
    +sku: STRING
    +product_type_id: STRING
    +units: BIGINT
    +developer_proceeds: DOUBLE
    +customer_currency: STRING
    +country: STRING
    +version: STRING
    +title: STRING
    +report_date: DATE
    <<partition>> dt: STRING
  }

```

---

## 10) Error handling & SLAs (ops loop)

```mermaid
flowchart TD
  A["Poller error / 409 from Apple"] --> B["Log full JSON<br/>(classify: auth/rate/attribute)"]
  B --> C{"Retry Policy"}
  C -- "429/5xx" --> D["Exponential Backoff + Jitter"]
  C -- "4xx schema" --> E["Fix payload/reportType<br/>resubmit"]
  D --> F["Continue"]
  E --> F["Continue"]
  F --> G["Alert if window stale > 24h"]

```

---

### That’s the whole picture

* You can drop these into your docs or PRDs as-is.
* If you want, I can generate a **single PDF** with all diagrams or a **README.md** that your team can commit to the repo.
