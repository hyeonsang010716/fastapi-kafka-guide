"""
알림 발송 시뮬레이터 (notifier)
실제 이메일/SMS/푸시 대신 콘솔 로그로 알림을 시뮬레이션

프로덕션 환경에서는 이 모듈을 실제 알림 서비스 연동으로 교체:
- SendGrid / AWS SES (이메일)
- Twilio (SMS)
- Firebase Cloud Messaging (푸시)
"""

import uuid
import logging
from typing import List, Dict, Any
from datetime import datetime

logger = logging.getLogger("notification-service.notifier")

# ──────────────────────────────────────────────
# 발송된 알림 기록 (인메모리)
# ──────────────────────────────────────────────
sent_notifications: List[Dict[str, Any]] = []


def send_notification(
    order_id: str,
    event_type: str,
    message: str,
    user_id: str | None = None,
    channel: str = "console",
) -> Dict[str, Any]:
    """
    알림 발송 시뮬레이션
    실제 구현에서는 외부 알림 서비스 API 호출로 대체

    Args:
        order_id: 관련 주문 ID
        event_type: 트리거한 이벤트 타입
        message: 알림 메시지 내용
        user_id: 알림 대상 사용자 ID
        channel: 알림 채널 (console, email, sms, push)

    Returns:
        발송된 알림 정보 딕셔너리
    """
    notification_id = str(uuid.uuid4())[:8]
    now = datetime.utcnow().isoformat()

    notification = {
        "notification_id": notification_id,
        "order_id": order_id,
        "user_id": user_id,
        "event_type": event_type,
        "channel": channel,
        "message": message,
        "status": "sent",
        "sent_at": now,
    }

    # 콘솔에 알림 출력 (시뮬레이션)
    logger.info("=" * 60)
    logger.info("  [알림 발송] ID: %s", notification_id)
    logger.info("  채널: %s | 대상: %s", channel, user_id or "전체")
    logger.info("  주문: %s | 이벤트: %s", order_id, event_type)
    logger.info("  메시지: %s", message)
    logger.info("=" * 60)

    # 기록 저장
    sent_notifications.append(notification)

    return notification


def build_message(event_type: str, event_data: dict) -> str:
    """
    이벤트 타입에 따라 사용자 친화적인 알림 메시지 생성

    Args:
        event_type: 이벤트 타입
        event_data: 이벤트 데이터

    Returns:
        사용자에게 보낼 메시지 문자열
    """
    order_id = event_data.get("order_id", "알 수 없음")

    messages = {
        "ORDER_CREATED": (
            f"주문이 접수되었습니다! (주문번호: {order_id}) "
            f"결제 및 재고 확인 중입니다."
        ),
        "PAYMENT_COMPLETED": (
            f"결제가 완료되었습니다! (주문번호: {order_id}) "
            f"금액: {event_data.get('amount', 0):,.0f}원"
        ),
        "PAYMENT_FAILED": (
            f"결제에 실패했습니다. (주문번호: {order_id}) "
            f"사유: {event_data.get('reason', '알 수 없는 오류')}"
        ),
        "INVENTORY_RESERVED": (
            f"상품이 준비되었습니다! (주문번호: {order_id}) "
            f"곧 배송이 시작됩니다."
        ),
        "INVENTORY_FAILED": (
            f"죄송합니다. 재고가 부족합니다. (주문번호: {order_id}) "
            f"사유: {event_data.get('reason', '재고 부족')}"
        ),
    }

    return messages.get(event_type, f"주문 {order_id}에 대한 업데이트가 있습니다.")
