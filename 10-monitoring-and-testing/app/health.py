"""
Kafka 헬스체크 모듈
- 브로커 연결 상태 확인
- 컨슈머 그룹 랙(lag) 조회
- 클러스터 메타데이터 조회
"""

from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient

from app.config import settings
from app.logging_config import get_logger

logger = get_logger(__name__)


class KafkaHealthChecker:
    """
    Kafka 클러스터의 상태를 점검하는 헬스체커 클래스
    - 브로커 연결 확인
    - 컨슈머 랙 모니터링
    - 클러스터 정보 조회
    """

    def __init__(self, bootstrap_servers: str | None = None) -> None:
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers

    async def check_broker_connection(self) -> dict:
        """
        Kafka 브로커에 연결을 시도하여 상태를 확인

        Returns:
            연결 상태 정보 딕셔너리
            - connected: 연결 성공 여부
            - broker: 브로커 주소
            - error: 에러 메시지 (실패 시)
        """
        try:
            # AdminClient를 사용하여 브로커 연결 테스트 (Consumer보다 가볍고 안정적)
            admin_client = AIOKafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
            )
            await admin_client.start()
            await admin_client.close()

            logger.info("broker_health_check_passed", broker=self.bootstrap_servers)
            return {
                "connected": True,
                "broker": self.bootstrap_servers,
            }
        except Exception as e:
            logger.error(
                "broker_health_check_failed",
                broker=self.bootstrap_servers,
                error=str(e),
            )
            return {
                "connected": False,
                "broker": self.bootstrap_servers,
                "error": str(e),
            }

    async def get_consumer_lag(self) -> dict:
        """
        컨슈머 그룹의 랙(lag)을 조회

        Consumer Lag = 토픽의 최신 오프셋 - 컨슈머 그룹의 커밋된 오프셋
        랙이 크면 컨슈머가 메시지 처리를 따라가지 못하고 있다는 의미

        Returns:
            파티션별 랙 정보 딕셔너리
        """
        lag_info: dict = {"group_id": settings.kafka_consumer_group, "partitions": []}
        total_lag = 0

        try:
            # 컨슈머를 생성하여 오프셋 정보 조회
            consumer = AIOKafkaConsumer(
                settings.kafka_topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=settings.kafka_consumer_group,
                enable_auto_commit=False,
            )
            await consumer.start()

            try:
                # 할당된 파티션 정보 조회
                partitions = consumer.assignment()

                for tp in partitions:
                    # 각 파티션의 최신 오프셋 (end offset)
                    end_offsets = await consumer.end_offsets([tp])
                    end_offset = end_offsets[tp]

                    # 컨슈머 그룹이 커밋한 오프셋
                    committed = await consumer.committed(tp)
                    committed_offset = committed if committed is not None else 0

                    # 랙 계산
                    partition_lag = end_offset - committed_offset
                    total_lag += partition_lag

                    lag_info["partitions"].append(
                        {
                            "topic": tp.topic,
                            "partition": tp.partition,
                            "end_offset": end_offset,
                            "committed_offset": committed_offset,
                            "lag": partition_lag,
                        }
                    )
            finally:
                await consumer.stop()

            lag_info["total_lag"] = total_lag

            logger.info(
                "consumer_lag_checked",
                group_id=settings.kafka_consumer_group,
                total_lag=total_lag,
            )

        except Exception as e:
            logger.error("consumer_lag_check_failed", error=str(e))
            lag_info["error"] = str(e)

        return lag_info

    async def get_cluster_info(self) -> dict:
        """
        AIOKafkaAdminClient를 사용하여 클러스터 메타데이터를 조회

        Returns:
            클러스터 정보 딕셔너리
            - brokers: 브로커 목록
            - topics: 토픽 목록 및 파티션 정보
            - controller: 컨트롤러 브로커 ID
        """
        try:
            admin_client = AIOKafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
            )
            await admin_client.start()

            try:
                # 클러스터 메타데이터 조회
                metadata = await admin_client.describe_cluster()

                # 토픽 목록 조회
                topics = await admin_client.list_topics()

                cluster_info = {
                    "brokers": [
                        {"node_id": b["node_id"], "host": b["host"], "port": b["port"]}
                        for b in metadata["brokers"]
                    ],
                    "controller_id": metadata["controller_id"],
                    "topics": list(topics),
                }

                logger.info(
                    "cluster_info_retrieved",
                    broker_count=len(cluster_info["brokers"]),
                    topic_count=len(cluster_info["topics"]),
                )
                return cluster_info
            finally:
                await admin_client.close()

        except Exception as e:
            logger.error("cluster_info_retrieval_failed", error=str(e))
            return {"error": str(e)}


# 싱글톤 헬스체커 인스턴스
health_checker = KafkaHealthChecker()
