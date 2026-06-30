"""FSM states for the Telegram bot."""

from aiogram.fsm.state import State, StatesGroup


class SettingsStates(StatesGroup):
    managing_domains = State()
    editing_domain = State()
    confirming_delete = State()


class GroupStates(StatesGroup):
    entering_name = State()
    adding_channel = State()


class DigestStates(StatesGroup):
    choosing_hour = State()
    choosing_scope = State()


class AddSourceStates(StatesGroup):
    waiting_input = State()  # link / @username / chat id, resolved via the pool
