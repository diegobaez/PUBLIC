#NiFi Custom processor

Use Spark as part of a NiFi Flow
- Ability to launch Spark Processing from NiFi
- Launch Mode:
  * Synchronous Spark Job: Launch-wait-get results
  * Async Spark Job: Launch and Forget, Set Listener for results
- Send to:
  * Local spark instance
  * Remote Spark Instance
  * Multiple remote spark instances
  * Spark Load Balancer
  * Ephemeral Spark Cluster
  * [Advanced] Specify order of execution for launch to Spark Processing Cluster
