from database import get_session, Session, Setting

def get_setting(session: Session, key: str, default: str = "") -> str:
    s = session.get(Setting, key)
    return s.value if s else default


def set_setting(session: Session, key: str, value: str):
    s = session.get(Setting, key)
    if s:
        s.value = value
    else:
        s = Setting(key=key, value=value)
        session.add(s)
    session.commit()
