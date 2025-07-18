document.addEventListener('DOMContentLoaded', function() {
    // Auto-expand tables with errors
    document.querySelectorAll('.table-card.error details').forEach(details => {
        details.setAttribute('open', '');
    });
});