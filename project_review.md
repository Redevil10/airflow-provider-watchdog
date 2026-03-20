⏺ Project Review: airflow-provider-watchdog

  This is a well-structured Apache Airflow provider package that monitors DAG/task health by querying the metadata database. Here's my review:                                                    
   
  Strengths                                                                                                                                                                                       
                                                                                                                                                                                                
  - Zero external dependencies — only requires Airflow itself, no Prometheus/Grafana/Datadog                                                                                                      
  - Clean architecture — each detector follows a consistent interface: detect(session, config) -> list[Alert]
  - Statistical rigor — IQR-based anomaly detection is more robust than z-score approaches                                                                                                        
  - Good defaults — sensible configuration with easy JSON-based overrides via Airflow Variables                                                                                                   
  - Self-contained UI — dark-themed dashboard with auto-refresh, integrated via Flask Blueprint                                                                                                   
  - Proper CI — GitHub Actions with matrix testing (Python 3.10-3.12), ruff linting, and build artifact publishing                                                                                
                                                                                                                                                                                                  
  Areas for Improvement                                                                                                                                                                           
                                                                                                                                                                                                  
  1. Test coverage is thin                                                                                                                                                                        
  - Only 3 test files with 11 test cases covering config, alert enums, and provider info
  - No tests for the 4 detectors — the core business logic (runtime, failures, deadlines, stuck) is completely untested                                                                           
  - No tests for alerting dispatch, the DAG definition, or the UI blueprint                                                                                                                     
  - Recommend adding integration tests with a real PostgreSQL session or at minimum mocked query results                                                                                          
                                                                                                                                                                                                  
  2. PostgreSQL-only                                                                                                                                                                              
  - All detectors use PERCENTILE_CONT which is PostgreSQL-specific                                                                                                                                
  - This is documented but limits adoption. Consider fallback to approximate percentiles or Python-side computation for broader compatibility                                                     
                                                                                                                                                                                                
  3. SQL injection risk                                                                                                                                                                           
  - Would need to verify that the exclude_dags list and other config values are properly parameterized in the raw SQL queries. The IN :exclude_dags tuple binding can behave differently across 
  SQLAlchemy versions                                                                                                                                                                             
                                                                                                                                                                                                
  4. XCom-based alert storage is fragile                                                                                                                                                          
  - Dashboard data is limited to the latest watchdog run's XCom output                                                                                                                            
  - Alert history is lost on each run — no trending or historical analysis possible                                                                                                               
  - The roadmap mentions a dedicated table, which would be a significant improvement                                                                                                              
                                                                                                                                                                                                  
  5. Dashboard template uses string replacement instead of Jinja                                                                                                                                  
  - dashboard.html injects data via simple string replacement, which could be fragile and risks XSS if any alert messages contain untrusted content                                               
                                                                                                                                                                                                  
  6. Error handling in detectors                                                                                                                                                                
  - If one detector fails, it's unclear whether the others still run. The DAG task calls them sequentially — a failure in one could prevent alerts from the rest                                  
                                                                                                                                                                                                  
  7. Missing py.typed marker and type annotations                                                                                                                                                 
  - The codebase uses dataclasses but could benefit from stricter typing, especially on the detector return types                                                                                 
                                                                                                                                                                                                  
  8. No CHANGELOG.md or versioning strategy                                                                                                                                                       
  - At v0.1.0 this is fine, but worth establishing early                                                                                                                                          
                                                                                                                                                                                                  
  Summary                                                                                                                                                                                         
                                                                                                                                                                                                  
  A solid v0.1 with clean design and practical value. The biggest gap is test coverage for the detectors — that's where the complex logic lives and where bugs would be most impactful. I'd       
  prioritize that along with XSS protection in the dashboard before any public release. Want me to help with any of these improvements?
                                                                                                                                                                                                   