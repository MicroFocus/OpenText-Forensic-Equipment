#!/usr/bin/env python3
################################################################################
# ot-tree-hashing.py
#
# This utility provides the ability to calculate the tree hash of an input file.
# This can be used to validate other tree hashing implementations. If requested
# the full tree state can be exported to aid debugging.
#
# Requires Python 3, tested on Python 3.10.0
#
# SPDX-License-Identifier: MIT
################################################################################

import os
import sys
import argparse

import json
import struct
import hashlib


################################################################################
# Utility functions
################################################################################
def parse_int(val):
    if not isinstance(val, str):
        return None

    try:
        return int(val)
    except ValueError:
        try:
            return int(val, base=16)
        except ValueError:
            return None


def I2OSP(val):
    # This is essentially converting <val> to an arbitrary length (<= 256) big
    # endian encoded integer stored in a string. We don't actually need to
    # encode an arbitrary length number, so we fix it at 8-bytes (uint64_t).
    return struct.pack('>QB', val, 8)


def format_block_to_str(block):
    if len(block) > 16:
        head = ''.join(['%02x' % x for x in block[:8]])
        tail = ''.join(['%02x' % x for x in block[-8:]])
        return head + '...' + tail
    else:
        return ''.join(['%02x' % x for x in block])


################################################################################
# This class manages reading the input and splitting it into the requested block
# size.
################################################################################
class BlockReader:
    def __init__(self, input_file, input_size, block_size):
        self._input_file = input_file
        self._input_size = input_size
        self._block_size = block_size

        self._input_pos = 0

        if self._input_file == 'ZERO':
            self._read_fn = self._zero_reader
            self._input = None
        else:
            self._read_fn = self._file_reader
            self._input = open(self._input_file, 'rb')

            file_size = os.path.getsize(self._input_file)
            if (self._input_size is None) or (self._input_size > file_size):
                self._input_size = file_size

    def _zero_reader(self):
        remaining = self._input_size - self._input_pos
        bytes_this_time = min(remaining, self._block_size)
        self._input_pos += bytes_this_time
        return bytes(bytes_this_time)

    def _file_reader(self):
        remaining = self._input_size - self._input_pos
        bytes_this_time = min(remaining, self._block_size)
        self._input_pos += bytes_this_time
        return self._input.read(bytes_this_time)

    def reset(self):
        self._input_pos = 0
        if self._input is not None:
            self._input.seek(0, 0)

    def next_block(self):
        return self._read_fn()

    def block_size(self):
        return self._block_size

    def input_size(self):
        return self._input_size

    def bytes_read(self):
        return self._input_pos


def BlockIterator(reader):
    while True:
        data = reader.next_block()
        if len(data) == 0:
            break
        yield data


################################################################################
# Performs a standard sequential hash of the input data using the specified hash
# algorithm.
################################################################################
def sequential_hash(hash_alg_name, reader):
    result = {}
    hash_alg = hashlib.new(hash_alg_name)

    for block in BlockIterator(reader):
        hash_alg.update(block)

    result['digest'] = hash_alg.hexdigest()
    return result


################################################################################
# Performs a final node growing tree hash of the input data using the specified
# hash algorithm.
################################################################################
def _fngt_hash_block(hash_alg_name, block, final_alg):
    # Calculate the new message hop
    hash_alg = hashlib.new(hash_alg_name)
    hash_alg.update(block)
    hash_alg.update(b'\x03')

    # Add it to the final message hop
    final_alg.update(hash_alg.digest())

    # Return this blocks hash
    result = {}
    result['input'] = format_block_to_str(block) + ' 03'
    result['chaining_value'] = hash_alg.hexdigest()
    return result

def _fngt_hash_finalize(block_count, final_alg):
    final_alg.update(I2OSP(block_count))
    final_alg.update(b'\xFF')
    final_alg.update(b'\xFF')
    final_alg.update(b'\x06')

    result = {}
    result['input'] = '<CV0 .. CV%d> ' % (block_count-1) + format_block_to_str(I2OSP(block_count)) + ' FF FF 06'
    result['digest'] = final_alg.hexdigest()
    return result

def fngt_tree_hash(hash_alg_name, reader):
    result = {}
    result['blocks'] = {}
    final_alg = hashlib.new(hash_alg_name)
    for block_index, block in enumerate(BlockIterator(reader)):
        result['blocks'][block_index] = _fngt_hash_block(hash_alg_name, block, final_alg)
    result['final'] = _fngt_hash_finalize(block_index+1, final_alg)
    return result


################################################################################
# main
################################################################################
def main():
    parser = argparse.ArgumentParser(description='Generate tree hashes for a given input.')

    # Hash parameters
    parser.add_argument('--md5', action='store_true', dest='md5',
        default=False, help='Enable MD5 hashing')
    parser.add_argument('--sha1', action='store_true', dest='sha1',
        default=False, help='Enable SHA1 hashing')
    parser.add_argument('--sha256', action='store_true', dest='sha256',
        default=False, help='Enable SHA256 hashing')
    parser.add_argument('--all-algs', action='store_true', dest='all_algs',
        default=False, help='Enable all supported hash algorithms')

    parser.add_argument('--sequential', action='store_true', dest='en_sequential',
        default=False, help='Enable standard sequential hash')
    parser.add_argument('--fng-tree', action='store_true', dest='en_fng_tree',
        default=False, help='Enable final node growing tree hash')
    parser.add_argument('--all-modes', action='store_true', dest='en_all_modes',
        default=False, help='Enable all implemented hash modes')

    parser.add_argument('--block-size-exponent', metavar='N', action='store',
        default=19, help='Specify block size exponent (block size is 2^N) [default=19]')

    # Optional output file
    parser.add_argument('--output', metavar='FILE', action='store', default=None, 
        help='Output file to write intermediate debug hash state, "-" for stdout')

    # Input file
    parser.add_argument('--input-size', metavar='SIZE', action='store', default=None,
        help='Read only the first SIZE bytes of the input, this argument is required for the ZERO input')
    parser.add_argument('input', metavar='FILE',
        help='Input file to read from, "ZERO" for virtual input of zeroes')

    # Parse the arguments
    args = parser.parse_args()

    args.block_size_exponent = \
        parse_int(args.block_size_exponent) if isinstance(args.block_size_exponent, str) \
            else args.block_size_exponent

    args.block_size = 2 ** args.block_size_exponent
    args.input_size = parse_int(args.input_size)

    # Validate the arguments
    any_hash_enabled = args.md5 or args.sha1 or args.sha256 or args.all_algs
    if not any_hash_enabled:
        sys.stderr.write('Error: at least one of [--md5, --sha1, --sha256] must be specified\n')
        sys.exit(1)

    any_mode_enabled = args.en_sequential or args.en_fng_tree or args.en_all_modes
    if not any_mode_enabled:
        sys.stderr.write('Error: at least one of [--sequential, --fng-tree] must be specified\n')
        sys.exit(1)

    if args.input == 'ZERO':
        if args.input_size == None:
            sys.stderr.write('Error: you must specify "--input-size" when using the "ZERO" input\n')
            sys.exit(1)
    else:
        if not os.path.exists(args.input):
            sys.stderr.write('Error: input file does not exist\n')
            sys.exit(1)

    # Prepare the input file
    reader = BlockReader(args.input, args.input_size, args.block_size)

    # Run the requested hash operations
    hash_algs = []
    if args.md5 or args.all_algs:
        hash_algs.append('md5')
    if args.sha1 or args.all_algs:
        hash_algs.append('sha1')
    if args.sha256 or args.all_algs:
        hash_algs.append('sha256')

    hash_modes = []
    if args.en_sequential or args.en_all_modes:
        hash_modes.append(('sequential', sequential_hash))
    if args.en_fng_tree or args.en_all_modes:
        hash_modes.append(('fng-tree', fngt_tree_hash))

    hash_state = {}
    for alg in hash_algs:
        hash_state[alg] = {}
        for mode in hash_modes:
            reader.reset()
            hash_state[alg][mode[0]] = mode[1](alg, reader)

    # Display the results
    print_results = (args.output is None) or (args.output != '-')
    output_to_stdout = args.output == '-'
    output_to_file = args.output is not None and not output_to_stdout

    if print_results:
        for alg_name, alg_result in hash_state.items():
            for mode, result in alg_result.items():
                if mode == 'sequential':
                    digest = result['digest']
                elif mode == 'fng-tree':
                    digest = result['final']['digest']
                else:
                    digest = 'Unknown hash mode'
                print('[%6s, %12s] = %s' % (alg_name, mode, digest))

    if output_to_stdout or output_to_file:
        hash_state_str = json.dumps(hash_state, sort_keys=True, indent=4)

    if output_to_stdout:
        sys.stdout.write(hash_state_str)
        sys.stdout.flush()

    if output_to_file:
        out_file = os.path.realpath(args.output)
        out_dir = os.path.dirname(out_file)
        os.makedirs(out_dir, exist_ok = True)
        with open(args.output, 'w') as f:
            f.write(hash_state_str)

    sys.exit(0)


if __name__ == '__main__':
    main()
