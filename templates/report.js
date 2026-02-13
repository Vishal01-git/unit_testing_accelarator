document.addEventListener('DOMContentLoaded', function() {
    document.querySelectorAll('.table-card.error details').forEach(details => {
        details.setAttribute('open', '');
    });
});