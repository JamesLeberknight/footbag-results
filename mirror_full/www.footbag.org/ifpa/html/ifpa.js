function old_ifpaJoinShirt()
{
  shirtCheckbox = document.getElementById("TShirt");
  shirtSelected = (shirtCheckbox.checked);

  addressDiv = document.getElementById("IfpaJoinShipping");

  if (shirtSelected)
    addressDiv.style.display = "block";
  else
    addressDiv.style.display = "none";
}

function ifpaGetDiv(divID)
{
  result = document.getElementById(divID);
  return (result ? result.innerHTML : "");
}

function ifpaJoinShirt()
{
  shirtCheckbox = document.getElementById("TShirt");
  shirtSelected = (shirtCheckbox.checked);

  addressDiv = document.getElementById("IfpaJoinShipping");

  if (shirtSelected)
    addressDiv.innerHTML = "<div id=IfpaJoinShipping_style>"+ifpaGetDiv("IfpaJoinShipping_proto")+"</div>";
  else
    addressDiv.innerHTML = "";
}

function ifpaJoinPayment()
{
  return; // Temporarily disabled ; TODO fix this up
  paymentDiv = document.getElementById("IfpaJoinPayment");
  paymentDiv.innerHTML = "<div id=IfpaJoinPayment_style>"+ifpaGetDiv("IfpaJoinPayment_proto")+"</div>";

  paymentButton = document.getElementById("IfpaJoinButton");
  paymentButton.style.display = "none";
}
