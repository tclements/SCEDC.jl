using Documenter, SCEDC

makedocs(;
    modules=[SCEDC],
    format=Documenter.HTML(),
    pages=[
        "Home" => "index.md",
    ],
    repo="https://github.com/tclements/SCEDC.jl/blob/{commit}{path}#L{line}",
    sitename="SCEDC.jl",
    authors="Tim Clements <thclements@g.harvard.edu>",
    assets=String[],
)

deploydocs(;
    repo="github.com/tclements/SCEDC.jl",
)
