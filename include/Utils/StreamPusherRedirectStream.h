/**
 * @file StreamPusherRedirectStream.h
 * @date 28. November 2017
 *
 * @author Hung Tran
 */

#pragma once

/**
 * Connects a producer implemented as a stream to an consumer which accepts
 * elements using a push() method.
 */
template<class InStream, class OutReceiver, class SequenceReceiver>
class StreamPusherRedirectStream {
	InStream &_input;
	OutReceiver &_output;
	SequenceReceiver &_redirect;

public:
	StreamPusherRedirectStream(InStream &input, OutReceiver &output, SequenceReceiver &redirect,
							 bool start_now = true)
	  : _input(input), _output(output), _redirect(redirect) {
	  if (start_now)
		process();
	}

	void process() {
	  for (; !_input.empty(); ++_input) {
			_output.push(*_input);
			_redirect.push(*_input);
		}
	}
};
