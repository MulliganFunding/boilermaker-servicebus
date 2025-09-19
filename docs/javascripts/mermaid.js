/* Mermaid initialization */
document.addEventListener("DOMContentLoaded", function() {
  if (typeof mermaid !== 'undefined') {
    mermaid.initialize({
      startOnLoad: true,
      theme: window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'default',
      themeVariables: {
        primaryColor: '#1976d2',
        primaryTextColor: '#ffffff',
        primaryBorderColor: '#1565c0',
        lineColor: '#424242',
        sectionBkgColor: '#f5f5f5',
        altSectionBkgColor: '#ffffff',
        gridColor: '#e0e0e0',
        secondaryColor: '#ffcc02',
        tertiaryColor: '#fff'
      },
      flowchart: {
        useMaxWidth: true,
        htmlLabels: true,
        curve: 'linear'
      },
      sequence: {
        diagramMarginX: 50,
        diagramMarginY: 10,
        actorMargin: 50,
        width: 150,
        height: 65,
        boxMargin: 10,
        boxTextMargin: 5,
        noteMargin: 10,
        messageMargin: 35,
        mirrorActors: true,
        bottomMarginAdj: 1,
        useMaxWidth: true,
        rightAngles: false,
        showSequenceNumbers: false
      }
    });
  }
});