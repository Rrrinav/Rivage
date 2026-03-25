// --- GLOBAL SETUP & COLLEGE GUIDELINES ---
#set page(
  paper: "a4",
  margin: (left: 38mm, right: 25mm, top: 25mm, bottom: 25mm),

  // Custom double-line border perfectly framed around the text area
  // Border is exactly 10mm away from the text on all sides
  background: place(top + left, dx: 28mm, dy: 15mm)[
    // Outer thick border (3pt)
    #rect(width: 100% - 43mm, height: 100% - 30mm, stroke: 3pt)[
      // Inner thin border (0.5pt)
      #place(center + horizon)[
        #rect(width: 100% - 0.2pt, height: 100% - 0.2pt, stroke: 0.5pt)
      ]
    ]
  ]
)

#set text(
  font: "Times New Roman",
  size: 12pt,
)

// 1.5 line spacing equivalent (12pt font + 9pt leading = 21pt baseline distance)
#set par(
  leading: 0.75em,
  justify: true,
)

#set heading(numbering: "1.")

// --- TITLE PAGE ---
#align(center)[
  #v(2cm)
  #text(size: 12pt, weight: "bold")[Rivage: A Polyglot Distributed Compute Engine for Numerical and Batch Processing] \
  #v(1.5cm)
  #text(size: 12pt, weight: "bold")[Final Year Engineering Project Report] \
  #v(1cm)
  #text(size: 12pt, style: "italic")[Draft Version] \
  #v(2cm)
]

#pagebreak()

// --- TABLE OF CONTENTS ---
#outline(title: "Table of Contents", depth: 2)

#pagebreak()

// --- MAIN CONTENT ---

// Define a custom footer to precisely center the page numbers 
// in the gap between the text block and the bottom border
#set page(
  footer: context align(center)[
    #move(dy: -6mm)[
      #counter(page).display("1")
    ]
  ]
)
#counter(page).update(1)

= Introduction


With the growing technology, the amount of data grows and with that grows the amount of compute we need to process the humongous data. CPUs and GPUs have a limit on how much they can process and a most normal computers/devices have a limit on how many processing units they can hold. Thus processing so much data in a congested computer is tough. This breeds the need for a distributed compute, i.e using multiple scattered devices to do the computations.

Obviously, the tech industry has had these problems for a long time and they have figured out industry grade solutions for it already, these include systems like Hadoop, which in general are very monolithic, complicated, hard to setup and heavily coupled to specific ecosystems like Java Virtual Machine (JVM). This rigit architecture leads to steep learning curves, high memory footprints, difficult to roll-up lightweight scripts or use other programming languages without relying on inefficient wrappers. For many general-purpose distributed computing tasks, deploying such a heavy architecture is unnecessary and cumbersome.

To solve these challenges, this project introduces a distributed computing framework, that is a lightweight, high-throughput distributed MapReduce orchestration engine. Rivage is built from the ground up using Go (Golang), leveraging the language's native concurrency model and high-performance networking capabilities. Our focus is on using modern technologies like GRPC, stackful coroutines, protobuff etc.

This project uses polyglot execution environment using scripts written in any language (generally simple scripting languages) to write the executable code and treating worker nodes as agnostic execution vessels. We use a mix of json and binary data and a centralized data store to save and update the data at. This design will allow users to write the map and reduce function in language of their choice and orchestration logic in golang itself.

== Objectives
The primary objectives of this project are as follows:
- To design and implement a lightweight, distributed MapReduce orchestration engine focusing on high concurrency and minimal operational overhead.
- To build a highly responsive communication layer using bi-directional gRPC streams for real-time task dispatching and state synchronization.
- To architect a language-agnostic worker node that utilizes standard I/O piping, allowing tasks to be executed in any programming language without framework-specific SDKs.
- To demonstrate the engine's capability by successfully distributing and executing general-purpose batch processing workloads.

#pagebreak()
= Literature Review
[To be written: Review of existing distributed systems like Hadoop, Apache Spark, and standard MapReduce papers. Will discuss their advantages and the operational complexities/lock-ins they face.]

#pagebreak()
= Design, Setup and Methodology
[To be written: Will contain the architectural diagrams, the Go gRPC communication flow, and the implementation details of the polyglot worker piping JSON via `os/exec`.]

#pagebreak()
= Results and Discussion
[To be written: Performance benchmarks, evaluation of MapReduce jobs (e.g., word count, batch processing) running on distributed nodes. Analysis of system throughput and resource utilization.]

#pagebreak()
= Conclusions and Scope for Future Work
[To be written: Summary of findings regarding the efficiency of the polyglot approach. Future scope will include implementing fault tolerance, advanced load-aware scheduling, and external disk-based shuffling.]

// --- PAGE BREAK FOR BIO ---
#pagebreak()
#heading(numbering: none)[Brief Bio Data of the Candidate]
[To be written: One page strictly containing the candidate's academic background, skills, and project contributions.]

// --- PAGE BREAK FOR PUBLICATIONS ---
#pagebreak()
#heading(numbering: none)[Research Publications]
[To be written: List of any papers or journals published derived from this project, if applicable.]

// --- PAGE BREAK FOR REFERENCES ---
#pagebreak()
#heading(numbering: none)[References]
[1] J. Dean and S. Ghemawat, "MapReduce: Simplified Data Processing on Large Clusters," _Communications of the ACM_, vol. 51, no. 1, pp. 107-113, 2008. \
\
[2] gRPC Authors, "gRPC: A high performance, open source universal RPC framework," [Online]. Available: https://grpc.io/

// --- PAGE BREAK FOR APPENDICES ---
#pagebreak()
#set heading(numbering: "A.")
#counter(heading).update(0)

= Appendices
[To be written: Tables, extended code snippets (e.g., Protobuf definitions, worker core logic), and supplementary data not included in the main report.]
