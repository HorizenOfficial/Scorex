Sparkz - The modular blockchain framework
- Dev : [![Build Status: dev](https://app.travis-ci.com/HorizenOfficial/Sparkz.svg?branch=dev)](https://app.travis-ci.com/HorizenOfficial/Sparkz) [![Coverage Status](https://coveralls.io/repos/github/HorizenOfficial/Sparkz/badge.svg?branch=dev)](https://coveralls.io/github/HorizenOfficial/Sparkz?branch=dev)
- Main: [![Build Status: dev](https://app.travis-ci.com/HorizenOfficial/Sparkz.svg?branch=main)](https://app.travis-ci.com/HorizenOfficial/Sparkz) [![Coverage Status](https://coveralls.io/repos/github/HorizenOfficial/Sparkz/badge.svg?branch=main)](https://coveralls.io/github/HorizenOfficial/Sparkz?branch=main)
===========================================================================================================================================

Sparkz
-------------------

Sparkz 2 is modular blockchain framework is Scala language which allows for free and limitless experimentation with a 
wide variety of designs. 

It is a fork of Scorex framework, which can be found 
at [https://github.com/hyperledger-labs/Scorex](https://github.com/hyperledger-labs/Scorex).

Motivation
----------

 If you have a new design for a blockchain system, there are few options available in regards with an implementation:
 * you can fork codebase of a Bitcoin or Ethereum client. However, such clients are optimized towards concrete 
 protocol, thus implementing something different would be a cumbersome task.
 * there are some modular frameworks, such as Scorex, where you can change consensus layer, or transactional
   layer, or both. Still, these modules have concrete interfaces, so for many designs more low-level and abstract 
   approach was needed.

 We have read a lot of research papers to make Scorex 2 supporting their implementations. Its abstract core 
 allows for implementing a broad range of systems, including ones with multiple types of blocks and non-linear history.


Features
--------

 * Compact, functional code
 * Modular design with fine granularity
 * Scala language
 * Asynchronous networking layer on top of TCP
 * JSON API
 * Cryptographic primitives externalized into [separate scrypto framework](https://github.com/input-output-hk/scrypto)
 * Some examples provided, including one working in production

Documentation and Communication.
--------------------------------

Please join maillist at [https://groups.io/g/scorex-dev](https://groups.io/g/scorex-dev).
There is tutorial in progress, available at [https://github.com/ScorexFoundation/ScorexTutorial](https://github.com/ScorexFoundation/ScorexTutorial).

Examples
--------

There are two examples of blockchain systems built with Scorex:
 * [TreasuryCoin](https://github.com/input-output-hk/TreasuryCoin) is an experimental implementation of the perspective 
 treasury system described in [A Treasury System for Cryptocurrencies: Enabling Better Collaborative Intelligence](https://eprint.iacr.org/2018/435.pdf). 
 * [Ergo Platform](https://github.com/ergoplatform/ergo), a Proof-of-Work cryptocurrency and financial contracts 
 platform, with support for stateless nodes, different operating regimes with up to 4 possible sections in a block etc.

Development Plans
-----------------

Final 1.0 release of Scorex 2 is not done but near. Currently we're polishing and auditing the codebase towards the 
release. Then we will consider further plans, possibly including:

 * Improved networking layer
 * Support for Scala 3
 * More examples, possibly including non-linear systems (such as DAGs)

Contributions
-------------

Contributions are welcome! Please take a look into [issues](https://github.com/ScorexFoundation/ScorexTutorial/issues).
 Testing codebase is still not perfect at the moment, so writing a test is not just good for start, 
 but useful for the product as well.

New examples would be very helpful as well! 

Release
-------
To publish release version to Sonatype:
- make a tag with version number `vX.Y.Z` (used by `sbt-dynver` to set `version` in `build.sbt`);
- use the new tag to make a Github release, which triggers [`release.yml`](.github/workflows/release.yml) workflow and publishes release version to Sonatype;
