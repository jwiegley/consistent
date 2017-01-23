{ mkDerivation, base, lifted-async, lifted-base, monad-control
, stdenv, stm, transformers, transformers-base
, unordered-containers
}:
mkDerivation {
  pname = "consistent";
  version = "0.0.1";
  src = ./.;
  libraryHaskellDepends = [
    base lifted-async lifted-base monad-control stm transformers
    transformers-base unordered-containers
  ];
  testHaskellDepends = [ base lifted-async transformers ];
  description = "Eventually consistent STM transactions";
  license = stdenv.lib.licenses.mit;
}
