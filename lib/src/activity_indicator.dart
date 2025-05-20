import 'dart:async';

import 'package:rxdart/rxdart.dart';

class ActivityIndicator extends Stream<bool> implements ValueStream<bool> {
  final BehaviorSubject<int> _loadingCounter = BehaviorSubject.seeded(0);

  bool get value => _loadingCounter.value > 0;

  Stream<bool> get stream => _loadingCounter.map((event) => event > 0);

  void _increment() {
    _loadingCounter.add(_loadingCounter.value + 1);
  }

  void _decrement() {
    _loadingCounter.add(_loadingCounter.value - 1);
  }

  void close() {
    _loadingCounter.close();
  }

  @override
  StreamSubscription<bool> listen(
    void Function(bool event)? onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) =>
      stream.listen(
        onData,
        onError: onError,
        onDone: onDone,
        cancelOnError: cancelOnError,
      );

  bool get hasValue => _loadingCounter.hasValue;

  @override
  Object get error => _loadingCounter.error;

  @override
  Object? get errorOrNull => _loadingCounter.errorOrNull;

  @override
  bool get hasError => _loadingCounter.hasError;

  @override
  StackTrace? get stackTrace => _loadingCounter.stackTrace;

  @override
  bool? get valueOrNull {
    if (_loadingCounter.valueOrNull != null) {
      return _loadingCounter.value > 0;
    } else {
      return null;
    }
  }

  @override
  StreamNotification<bool>? get lastEventOrNull {
    final lastCounterEvent = _loadingCounter.lastEventOrNull;
    final lastValue = lastCounterEvent?.dataValueOrNull;

    if (lastValue != null) {
      return StreamNotification.data(lastValue > 0);
    }

    return null;
  }
}

class _ActivityIndicatorTransformer<T> extends StreamTransformerBase<T, T> {
  final StreamTransformer<T, T> _transformer;

  _ActivityIndicatorTransformer(ActivityIndicator indicator)
      : _transformer = StreamTransformer.fromHandlers(
          handleDone: (sink) {
            indicator._decrement();
            sink.close();
          },
        );

  @override
  Stream<T> bind(Stream<T> stream) => _transformer.bind(stream);
}

extension TrackActivity<T> on Stream<T> {
  Stream<T> trackActivity(ActivityIndicator activityIndicator) {
    activityIndicator._increment();
    return transform(_ActivityIndicatorTransformer<T>(activityIndicator));
  }

  Stream<T> whereNotLoading(ActivityIndicator activityIndicator) {
    return where((event) => activityIndicator.value != true);
  }
}
