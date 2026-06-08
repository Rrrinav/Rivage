# Report structure

Report structure according to the **NITSRI** official guidelines:

## Skeleton

### 1. Title Pages

* Outer title page
* Inner title page
* Copyright (on reverse side of inner title page)

### 2. Preliminary Pages (Page i, ii, iii, ...)

* Candidate’s declaration
* Acknowledgements
* Abstract
    This section will contain statement of the problem, methods of investigation, major findings and main conclusion.
* Contents
* List of figures
* List of tables
* List of abbreviations
* List of symbols

### 3. Main body pages (1, 2, 3, ...)

**Introduction**
This section will contain general introduction, scope of the work, objective of the study and chapter outline.

**Literature Review**
This section will contain a critical review of the literature, pertinent theory, experiment and the importance of the chosen problem.

**Design, Setup and Methodology**
The reporting on design, setup and methodology shall be presented in one or more chapters with appropriate chapter titles. Due importance shall be given to experimental setups, procedures adopted, techniques developed, methodologies developed and adopted.

**Results and Discussion**
* Brief description of the methodology, primarily the experimental design
* The text should describe the results
* The figure and table legends contains o Short title

**Description of the symbols, if applicable**
* Description of the statistics used, if applicable
* The figure or table and their legend should appear on the same page
* Avoid repeating a description of the results – keep the description of the results in the body of the results section and not in the figure or table legend
* Emphasize the most important contributions of the project. The discussion must not merely recapitulate results or review the literature
* It is essential to discuss the research in relationship to the literature and to assess the significance of the findings

**Conclusions and Scope for Future Work**
This section contains the major findings, main conclusions and future scope.

**Brief Bio Data of the Candidate (one page only)**
**Research Publications**

**References**
There must be only one reference list for the entire project report in order of citation in the body of project report. Preferably use IEEE format for references.

**Appendices**
This section may contain tables and figures of data that are necessary to show but that are not part of the project report.

**Description of Different Sections of a project**

## General guidelines
* Times New Roman font of size 12 must be followed consistently throughout the project report
* 1½ space throughout the text on both side of the paper
* Margins: Left - 38 mm, Right – 25 mm, Top – 25mm, Bottom – 25mm
* Paper size: A4
* Units and symbols should conform to the international system of units
* Avoid the use of jargon, nouns as adjectives, split infinitives, improper matching of subjects and verbs, changes of tense in mid-paragraph and redundancy and verbosity. More errors in spelling or typography leave an impression of carelessness on the examiners

### Auxiliary Format

**Binding**
* The evaluation copies of the project report may be spiral bound or soft bound. The final hard bound copies to be submitted after the oral examination will be accepted during the submission of project report with the following specification:

**Front and Back Covers’ Colour**
* Black

**Over Lettering**
* Front: Embossed in silver colour
* Side: Embossed in silver colour


3. PROPOSED METHODOLOGY

3.1 Problem Statement (We just wrote this)
3.2 System Architecture Overview * (High-level explanation of the decoupled Master-Worker model. You will include a large block diagram here showing nodes connected over a network).
3.3 The Control Plane: Master Node Architecture

    3.3.1 Directed Acyclic Graph (DAG) Execution Engine: (Explain how you convert user code into a topological graph. Include a diagram of a DAG).

    3.3.2 Dynamic Task Scheduling: (Explain how the Master decides where to send tasks. Detail your specific algorithms here, such as Round Robin and Least Loaded).

    3.3.3 State Management & Write-Ahead Logging (WAL): (Explain the exact logic of how you save state to disk before sending tasks. Include a state-machine diagram here).
    3.4 The Compute Plane: Polyglot Worker Architecture

    3.4.1 Native OS-Level Execution: (Explain how you use Go's os/exec to bypass JVMs and run Python/C++ scripts natively).

    3.4.2 I/O Piping and Serialization: (Explain how you pass JSON and binary data into the standard input/output of the native scripts).
    3.5 Network Communication Layer

    3.5.1 gRPC and Protocol Buffers: (Show a snippet of your .proto file and explain the bi-directional streams).

    3.5.2 Bounded-Memory Data Streaming: (Explain how you chunk large data into 4MB payloads to avoid OOM crashes. Include a sequence diagram of the data flow)
