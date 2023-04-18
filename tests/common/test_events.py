# noinspection PyUnresolvedReferences
import pytest
from unittest import TestCase
from centroid.common import SyncSeriesEventEmitter


def next_handler(event):
    event.total += 1


def once_handler(event):
    event.total += 1


def test_use_subscribe():
    event_series = SyncSeriesEventEmitter()
    subscription = event_series.subscribe(next_handler)
    TestCase().assertEqual(len(event_series.__handlers__), 1)
    event = lambda: None
    event.total = 100
    event_series.emit(event)
    TestCase().assertEqual(event.total, 101)
    event_series.emit(event)
    TestCase().assertEqual(event.total, 102)
    subscription.unsubscribe()
    TestCase().assertEqual(len(event_series.__handlers__), 0)


def test_use_subscribe_once():
    event_series = SyncSeriesEventEmitter()
    subscription1 = event_series.subscribe(next_handler)
    subscription2 = event_series.subscribe_once(next_handler)
    TestCase().assertEqual(len(event_series.__handlers__), 2)
    event = lambda: None
    event.total = 100
    event_series.emit(event)
    TestCase().assertEqual(event.total, 102)
    event_series.emit(event)
    TestCase().assertEqual(event.total, 103)
    event_series.emit(event)
    TestCase().assertEqual(event.total, 104)
    subscription1.unsubscribe()
    TestCase().assertEqual(len(event_series.__handlers__), 1)
    subscription2.unsubscribe()
