"""FSM states for the Telegram bot."""

from aiogram.fsm.state import State, StatesGroup


class AddChannelStates(StatesGroup):
    waiting_link = State()
    choosing_period = State()
    choosing_frequency = State()
    choosing_emoji = State()
    naming_batch_list = State()
    processing = State()


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
