# Hot-to-cold loader documentation

Start with [architecture](architecture.md) to understand the service, or [configuration](configuration.md) and [deployment](deployment.md) to run it.

| Guide | Audience and scope |
| --- | --- |
| [Architecture](architecture.md) | Users and developers: data flow, guarantees, and Parquet/Iceberg structure |
| [Configuration](configuration.md) | Operators: complete runtime settings and sibling-service integration |
| [Deployment](deployment.md) | Operators: writer migration, reader migration, bootstrap, and deletion rollout |
| [Operations](operations.md) | Operators: monitoring, restart, recovery, leases, and retained state |
| [Metrics](metrics.md) | Operators: Prometheus, Grafana, alerts, and profile retention |
| [Development](development.md) | Developers: source map, extension points, and verification requirements |
| [Validation](validation.md) | Developers and operators: native gates and performance policies |

The [repository README](../README.md) provides build commands and a bounded run example. The [testing index](../testing/README.md) describes the maintained harnesses. Historical design and measurements live in [plan 12](https://github.com/seanshahkarami/sage-data-rework/blob/main/plans/12_dynamic_archival_service.md).

## Resources

These references cover the warehouse layout, stored VARIANT values, and Iceberg v3 support used by the cold storage architecture:

- [DuckDB: the effect of Parquet row group sizes](https://duckdb.org/docs/2.0/guides/performance/file_formats#the-effect-of-row-group-sizes)
- [DuckDB 2.0: VARIANT becomes a first-class citizen](https://duckdb.org/docs/2.0/whats_new_in_the_docs#variant-becomes-a-first-class-citizen)
- [DuckDB VARIANT data type](https://duckdb.org/docs/2.0/sql/data_types/variant)
- [Apache Parquet VARIANT encoding specification](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)
- [VARIANT in Apache Iceberg](https://iceberg.apache.org/blog/variant-in-apache-iceberg/)
- [DuckDB Iceberg extension: Iceberg v3 support](https://duckdb.org/docs/2.0/core_extensions/iceberg/writing_to_iceberg#iceberg-v3-support)