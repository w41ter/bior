# bior

[![Build Status](https://travis-ci.org/thinkermao/bior.svg?branch=master)](https://travis-ci.org/thinkermao/bior)
[![Coverage Status](https://coveralls.io/repos/github/thinkermao/bior/badge.svg?branch=master)](https://coveralls.io/github/thinkermao/bior?branch=master)

Bior is a implements of raft consensus algorithm in go, it supported features:

- Leader Election (vote, pre vote)
- Log Replication, log compaction
- Membership change
- Read index
- Flow control

features implementing:

- Leadership transfer
- Lease read

DANGER: Without any check now.
