{ cabal, monadControl, transformers, transformersBase
, liftedBase, liftedAsync, unorderedContainers, stm
}:

cabal.mkDerivation (self: {
  pname = "consistent";
  version = "0.0.1";
  src = ./.;
  buildDepends = [
    monadControl transformers transformersBase liftedBase liftedAsync
    unorderedContainers stm
  ];
  testDepends = [ transformers liftedAsync ];
  meta = {
    description = "Eventually consistent STM transactions";
    license = self.stdenv.lib.licenses.mit;
    platforms = self.ghc.meta.platforms;
  };
})
