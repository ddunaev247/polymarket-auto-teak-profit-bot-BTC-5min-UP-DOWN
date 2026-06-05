let displayValue = '0';
let firstOperand = null;
let waitingForSecondOperand = false;
let currentOperator = null;

const display = document.getElementById('display');

function updateDisplay() {
    // Ограничиваем длину вывода, чтобы не вылезать за пределы экрана
    if (displayValue.length > 9) {
        display.innerText = displayValue.substring(0, 9);
    } else {
        display.innerText = displayValue;
    }
}

function inputDigit(digit) {
    if (waitingForSecondOperand === true) {
        displayValue = String(digit);
        waitingForSecondOperand = false;
    } else {
        displayValue = displayValue === '0' ? String(digit) : displayValue + digit;
    }
    updateDisplay();
}

function inputDecimal() {
    if (waitingForSecondOperand === true) {
        displayValue = '0.';
        waitingForSecondOperand = false;
        return updateDisplay();
    }
    if (!displayValue.includes('.')) {
        displayValue += '.';
    }
    updateDisplay();
}

function clearDisplay() {
    displayValue = '0';
    firstOperand = null;
    waitingForSecondOperand = false;
    currentOperator = null;
    updateDisplay();
}

function toggleSign() {
    displayValue = (parseFloat(displayValue) * -1).toString();
    updateDisplay();
}

function inputPercent() {
    displayValue = (parseFloat(displayValue) / 100).toString();
    updateDisplay();
}

function setOperator(nextOperator) {
    const inputValue = parseFloat(displayValue);

    if (currentOperator && waitingForSecondOperand) {
        currentOperator = nextOperator;
        return;
    }

    if (firstOperand == null && !isNaN(inputValue)) {
        firstOperand = inputValue;
    } else if (currentOperator) {
        const result = calculateValue(firstOperand, inputValue, currentOperator);
        displayValue = `${parseFloat(result.toFixed(7))}`;
        firstOperand = result;
    }

    waitingForSecondOperand = true;
    currentOperator = nextOperator;
    updateDisplay();
}

function calculateValue(firstOperand, secondOperand, operator) {
    if (operator === '+') return firstOperand + secondOperand;
    if (operator === '-') return firstOperand - secondOperand;
    if (operator === '*') return firstOperand * secondOperand;
    if (operator === '/') return firstOperand / secondOperand;
    return secondOperand;
}

function calculate() {
    if (currentOperator === null || waitingForSecondOperand) return;

    const inputValue = parseFloat(displayValue);
    const result = calculateValue(firstOperand, inputValue, currentOperator);

    displayValue = `${parseFloat(result.toFixed(7))}`;
    firstOperand = null;
    currentOperator = null;
    waitingForSecondOperand = false;
    updateDisplay();
}